"""
Hybrid TrickleClient - Combines John's separate queue architecture with dual-path audio processing.

This version takes the best of both approaches:
- John's separate video/audio queues (good organization)  
- John's video-only frame skipping (clean separation)
- My dual-path audio processing (solves timing issues)
- Removes complex sequencing (unnecessary with immediate audio)
"""

import asyncio
import logging
import time
import json
from typing import Callable, Optional, Union, Deque, Any
from collections import deque

from .protocol import TrickleProtocol
from .frames import VideoFrame, AudioFrame, VideoOutput, AudioOutput
from . import ErrorCallback
from .frame_processor import FrameProcessor
from .decoder import DEFAULT_MAX_FRAMERATE
from .frame_skipper import AdaptiveFrameSkipper, FrameSkipConfig, FrameProcessingResult

logger = logging.getLogger(__name__)

class HybridTrickleClient:
    """TrickleClient with hybrid architecture combining best practices."""
    
    def __init__(
        self,
        protocol: TrickleProtocol,
        frame_processor: 'FrameProcessor',
        control_handler: Optional[Callable] = None,
        send_data_interval: Optional[float] = 0.333,
        error_callback: Optional[ErrorCallback] = None,
        max_queue_size: int = 300,
        frame_skip_config: Optional[FrameSkipConfig] = None,
        enable_audio_transcription: bool = False
    ):
        """Initialize HybridTrickleClient with optimal audio/video handling."""
        self.protocol = protocol
        self.frame_processor = frame_processor
        self.control_handler = control_handler
        self.send_data_interval = send_data_interval
        self.error_callback = error_callback or frame_processor.error_callback
        
        # Configuration
        self.frame_skip_config = frame_skip_config
        self.max_queue_size = max_queue_size
        self.enable_audio_transcription = enable_audio_transcription
        
        # Connect protocol error callback
        if not self.protocol.error_callback:
            self.protocol.error_callback = self._on_protocol_error
        
        # Client state
        self.running = False
        self.request_id = "default"
        
        # Coordination events
        self.stop_event = asyncio.Event()
        self.error_event = asyncio.Event()
        
        # John's separate queues (good organization)
        self.video_input_queue = asyncio.Queue(maxsize=max_queue_size)
        self.audio_input_queue = asyncio.Queue(maxsize=max_queue_size * 4)  # Larger buffer for audio
        self.output_queue = asyncio.Queue(maxsize=200)
        self.data_queue: Deque[Any] = deque(maxlen=1000)
        
        # John's video frame skipper (video-only is correct)
        if frame_skip_config is not None:
            self.frame_skipper = AdaptiveFrameSkipper(
                config=frame_skip_config,
                fps_meter=protocol.fps_meter
            )
        else:
            self.frame_skipper = None
    
    async def start(self, request_id: str = "default"):
        """Start the hybrid client."""
        if self.running:
            raise RuntimeError("Client is already running")
            
        self.request_id = request_id
        self.stop_event.clear()
        self.error_event.clear()
        
        logger.info(f"Starting hybrid trickle client with request_id={request_id}")
        
        await self.protocol.start()
        self.running = True
        
        try:
            # Run all loops concurrently
            results = await asyncio.gather(
                self._ingress_loop(),
                self._process_video_frames(),   # John's video processing
                self._process_audio_frames_hybrid(),  # My dual-path audio processing
                self._egress_loop(), 
                self._control_loop(),
                self._send_data_loop(),
                return_exceptions=True
            )
            
            for i, result in enumerate(results):
                if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                    loop_names = ["ingress", "video_processing", "audio_processing", "egress", "control", "send_data"]
                    logger.error(f"{loop_names[i]} loop failed: {result}")
                    
        except Exception as e:
            logger.error(f"Error in client loops: {e}")
        finally:
            self.running = False
            if self.frame_processor.on_stream_stop:
                try:
                    await self.frame_processor.on_stream_stop()
                except Exception as e:
                    logger.error(f"Error in stream stop callback: {e}")
            await self.protocol.stop()
            if self.frame_skipper:
                self.frame_skipper.reset()
    
    async def _ingress_loop(self):
        """John's ingress loop - route frames to separate queues."""
        try:
            async for frame in self.protocol.ingress_loop(self.stop_event):
                if self.error_event.is_set() or self.stop_event.is_set():
                    break
                
                try:
                    if isinstance(frame, VideoFrame):
                        await self.video_input_queue.put(frame)
                    elif isinstance(frame, AudioFrame):
                        await self.audio_input_queue.put(frame)
                    else:
                        logger.warning(f"Unknown frame type received: {type(frame)}")
                except Exception as e:
                    logger.error(f"Error queueing frame: {e}")
            
            # Send sentinels to both queues
            await self.video_input_queue.put(None)
            await self.audio_input_queue.put(None)
            
        except Exception as e:
            logger.error(f"Error in ingress loop: {e}")
            self.error_event.set()
            self.stop_event.set()
    
    async def _process_video_frames(self):
        """John's video processing with frame skipping."""
        try:
            while not self.stop_event.is_set() and not self.error_event.is_set():
                try:
                    if self.frame_skipper:
                        try:
                            frame_or_result = await self.frame_skipper.process_video_queue(self.video_input_queue, timeout=5)
                            if frame_or_result is None:
                                break
                            elif frame_or_result == FrameProcessingResult.SKIPPED:
                                continue
                            frame = frame_or_result
                        except asyncio.TimeoutError:
                            continue
                    else:
                        try:
                            frame = await asyncio.wait_for(self.video_input_queue.get(), timeout=5.0)
                            if frame is None:
                                break
                        except asyncio.TimeoutError:
                            continue
                    
                    # Process video frame
                    processed_frame = await self.frame_processor.process_video_async(frame)
                    if processed_frame:
                        output = VideoOutput(processed_frame, self.request_id)
                        await self.output_queue.put(output)
                    
                except Exception as e:
                    logger.error(f"Error processing video frame: {e}")
                    
        except Exception as e:
            logger.error(f"Error in video processing loop: {e}")
    
    async def _process_audio_frames_hybrid(self):
        """Hybrid audio processing: immediate output + background transcription."""
        try:
            while not self.stop_event.is_set() and not self.error_event.is_set():
                try:
                    # Get audio frame from John's separate audio queue
                    try:
                        frame = await asyncio.wait_for(self.audio_input_queue.get(), timeout=5.0)
                        if frame is None:
                            logger.info("Audio processing received shutdown signal")
                            break
                    except asyncio.TimeoutError:
                        continue
                    
                    logger.debug(f"Hybrid audio processing: {frame.samples.shape}")
                    
                    # DUAL PATH SOLUTION:
                    # Path 1: Immediate output for perfect timing (ai-runner pattern)
                    output = AudioOutput([frame], self.request_id)
                    await self.output_queue.put(output)
                    
                    # Path 2: Background transcription processing (optional)
                    if self.enable_audio_transcription and hasattr(self.frame_processor, 'process_audio_async'):
                        asyncio.create_task(self._process_audio_for_transcription(frame))
                    
                except Exception as e:
                    logger.error(f"Error in hybrid audio processing: {e}")
                    
        except Exception as e:
            logger.error(f"Error in hybrid audio processing loop: {e}")
    
    async def _process_audio_for_transcription(self, frame: AudioFrame):
        """Background transcription processing without affecting timing."""
        try:
            logger.debug(f"Background transcription for frame: {frame.samples.shape}")
            
            # Process audio for transcription (background only)
            transcription_result = await self.frame_processor.process_audio_async(frame)
            
            if transcription_result:
                # Publish transcription data via data channel
                transcription_data = {
                    "type": "audio_transcription",
                    "timestamp": frame.timestamp,
                    "time_base": [frame.time_base.numerator, frame.time_base.denominator],
                    "sample_rate": frame.rate,
                    "nb_samples": frame.nb_samples
                }
                
                if isinstance(transcription_result, list):
                    transcription_data["processed_frames"] = len(transcription_result)
                
                await self.publish_data(json.dumps(transcription_data))
                logger.debug(f"Published transcription data for frame {frame.timestamp}")
            
        except Exception as e:
            logger.error(f"Error in background transcription: {e}")
            # Don't re-raise - background processing shouldn't affect main flow
    
    async def _egress_loop(self):
        """Simplified egress loop - no audio timestamp manipulation needed."""
        try:
            async def output_generator():
                while not self.stop_event.is_set() and not self.error_event.is_set():
                    try:
                        frame = await asyncio.wait_for(self.output_queue.get(), timeout=0.5)
                        if frame is not None:
                            # Audio frames now have perfect original timing
                            yield frame
                        else:
                            break
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logger.error(f"Error in output generation: {e}")
                        continue
                    
            await self.protocol.egress_loop(output_generator())
            
        except Exception as e:
            logger.error(f"Error in egress loop: {e}")
            self.error_event.set()
            self.stop_event.set()
    
    # ... other methods same as original TrickleClient ...
    
    async def stop(self):
        """Stop the hybrid client."""
        if not self.running:
            return
            
        logger.info("Stopping hybrid trickle client")
        self.stop_event.set()
        
        try:
            self.output_queue.put_nowait(None)
        except asyncio.QueueFull:
            pass
        
        self.data_queue.append(None)
    
    async def publish_data(self, data: str):
        """Publish data via the protocol's data publisher."""
        self.data_queue.append(data)
    
    def get_statistics(self) -> dict:
        """Get comprehensive statistics."""
        stats = {
            "video_input_queue_size": self.video_input_queue.qsize(),
            "audio_input_queue_size": self.audio_input_queue.qsize(),
            "output_queue_size": self.output_queue.qsize(),
            "audio_transcription_enabled": self.enable_audio_transcription
        }
        
        if self.frame_skipper:
            stats.update({
                "frame_skipper_enabled": True,
                "skip_interval": self.frame_skipper.skip_interval,
                "target_fps": self.frame_skipper.config.target_fps
            })
        else:
            stats["frame_skipper_enabled"] = False
            
        return stats
    
    # ... include other necessary methods from original implementation ...
