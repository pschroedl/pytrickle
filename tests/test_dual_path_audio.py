#!/usr/bin/env python3
"""
Test dual-path audio processing to verify:
1. Audio frames go immediately to output for timing preservation
2. Audio frames are also processed asynchronously for transcription
3. No audio distortion occurs due to async delays
"""

import asyncio
import logging
import time
from fractions import Fraction
from unittest.mock import Mock, AsyncMock
from pytrickle.client import TrickleClient
from pytrickle.protocol import TrickleProtocol
from pytrickle.frame_processor import FrameProcessor
from pytrickle.frames import VideoFrame, AudioFrame
from pytrickle.frame_skipper import FrameSkipConfig
import torch
import numpy as np
import av

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TranscriptionProcessor(FrameProcessor):
    """Test processor that simulates transcription processing."""
    
    def __init__(self, transcription_delay: float = 0.1):
        self.transcription_delay = transcription_delay
        self.processed_audio_count = 0
        self.processed_video_count = 0
        super().__init__()
    
    async def load_model(self, **kwargs):
        """Load model (no-op for test)."""
        logger.info("Transcription processor model loaded")
        pass
    
    async def process_video_async(self, frame: VideoFrame) -> VideoFrame:
        """Process video frame (simple passthrough)."""
        self.processed_video_count += 1
        return frame
    
    async def process_audio_async(self, frame: AudioFrame) -> list[AudioFrame]:
        """Simulate transcription processing with delay."""
        logger.info(f"Transcription processing started for frame {frame.timestamp}")
        start_time = time.time()
        
        # Simulate transcription delay
        await asyncio.sleep(self.transcription_delay)
        
        self.processed_audio_count += 1
        end_time = time.time()
        
        logger.info(f"Transcription processing completed in {end_time - start_time:.3f}s")
        
        # Return processed frame with simulated transcription metadata
        return [frame]
    
    def update_params(self, params: dict):
        """Update processing parameters."""
        if "transcription_delay" in params:
            self.transcription_delay = float(params["transcription_delay"])
            logger.info(f"Updated transcription delay to {self.transcription_delay}s")


async def create_test_audio_frames(num_frames: int = 5):
    """Create test audio frames with sequential timestamps."""
    frames = []
    
    for i in range(num_frames):
        # Create realistic audio timestamps (48kHz, 1024 samples per frame)
        timestamp = i * 1024  # Sequential audio timestamps
        
        # Create numpy samples
        audio_samples = np.random.rand(2, 1024).astype(np.float32)  # Stereo audio
        
        # Create av.AudioFrame
        av_audio_frame = av.AudioFrame.from_ndarray(audio_samples, format='fltp', layout='stereo')
        av_audio_frame.sample_rate = 48000
        av_audio_frame.pts = timestamp
        av_audio_frame.time_base = Fraction(1, 48000)
        
        # Create AudioFrame
        audio_frame = AudioFrame.from_av_audio(av_audio_frame)
        frames.append(audio_frame)
    
    return frames


async def test_dual_path_audio_processing():
    """Test that audio frames are processed via dual path without timing distortion."""
    logger.info("=== Testing Dual-Path Audio Processing ===")
    
    # Create transcription processor with delay
    processor = TranscriptionProcessor(transcription_delay=0.2)  # 200ms transcription delay
    
    # Create mock protocol
    protocol = Mock(spec=TrickleProtocol)
    protocol.start = AsyncMock()
    protocol.stop = AsyncMock()
    protocol.error_callback = None
    
    # Mock FPSMeter
    from pytrickle.fps_meter import FPSMeter
    protocol.fps_meter = FPSMeter()
    
    # Create test audio frames
    test_frames = await create_test_audio_frames(num_frames=5)
    
    # Track timing
    frame_output_times = []
    transcription_completion_times = []
    
    # Mock ingress loop 
    async def mock_ingress_loop(stop_event):
        logger.info("Mock ingress loop starting")
        for i, frame in enumerate(test_frames):
            if stop_event.is_set():
                break
            logger.info(f"Yielding audio frame {i+1} with timestamp {frame.timestamp}")
            yield frame
            await asyncio.sleep(0.05)  # Small delay between frames
        logger.info("Mock ingress loop completed")
    
    # Mock egress loop to track when frames are output
    async def mock_egress_loop(output_generator):
        frame_count = 0
        async for output_frame in output_generator:
            frame_output_times.append(time.time())
            frame_count += 1
            logger.info(f"Egress received frame {frame_count} at {time.time():.3f}")
    
    # Mock control loop
    async def mock_control_loop(stop_event):
        while not stop_event.is_set():
            await asyncio.sleep(0.1)
        if False:
            yield
    
    protocol.ingress_loop = mock_ingress_loop
    protocol.egress_loop = mock_egress_loop  
    protocol.control_loop = mock_control_loop
    protocol.publish_data = AsyncMock()  # Mock data publishing
    
    # Create client with transcription enabled
    client = TrickleClient(
        protocol=protocol,
        frame_processor=processor,
        enable_audio_transcription=True  # Enable dual-path processing
    )
    
    # Track transcription completion by monitoring publish_data calls
    original_publish_data = client.publish_data
    async def track_transcription_publish(data):
        transcription_completion_times.append(time.time())
        logger.info(f"Transcription completed at {time.time():.3f}")
        return await original_publish_data(data)
    client.publish_data = track_transcription_publish
    
    # Test execution
    start_time = time.time()
    
    try:
        await asyncio.wait_for(client.start("test_request"), timeout=3.0)
    except asyncio.TimeoutError:
        logger.info("Client stopped due to timeout (expected)")
    
    # Stop client cleanly
    await client.stop()
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Analysis
    logger.info(f"=== Dual-Path Audio Test Results ===")
    logger.info(f"Total execution time: {total_time:.3f}s")
    logger.info(f"Frames output immediately: {len(frame_output_times)}")
    logger.info(f"Transcriptions completed: {len(transcription_completion_times)}")
    logger.info(f"Audio frames processed by transcription: {processor.processed_audio_count}")
    
    # Verify timing characteristics
    if frame_output_times and transcription_completion_times:
        # Audio frames should be output immediately (before transcription completes)
        first_frame_time = frame_output_times[0] - start_time
        first_transcription_time = transcription_completion_times[0] - start_time
        
        logger.info(f"First frame output at: {first_frame_time:.3f}s")
        logger.info(f"First transcription at: {first_transcription_time:.3f}s")
        
        if first_frame_time < first_transcription_time:
            logger.info("✅ SUCCESS: Audio frames output before transcription completes (no timing distortion)")
        else:
            logger.warning("❌ ISSUE: Audio timing may be affected by transcription processing")
    
    # Verify all frames were processed
    if len(frame_output_times) >= len(test_frames):
        logger.info("✅ SUCCESS: All audio frames were output")
    else:
        logger.warning(f"❌ ISSUE: Only {len(frame_output_times)}/{len(test_frames)} frames output")
    
    # Verify background transcription worked
    if processor.processed_audio_count > 0:
        logger.info("✅ SUCCESS: Background transcription processing occurred")
    else:
        logger.warning("❌ ISSUE: No background transcription processing detected")


if __name__ == "__main__":
    asyncio.run(test_dual_path_audio_processing())
