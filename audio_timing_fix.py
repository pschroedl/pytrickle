"""
Audio Timing Fix for feat/frame-skipper

Replace John's _process_audio_frames() method with this implementation
to fix audio timing distortion while enabling transcription capabilities.
"""

import asyncio
import json
import logging

logger = logging.getLogger(__name__)

# FIXED: Replace John's _process_audio_frames() method with this
async def _process_audio_frames(self):
    """Process audio frames with immediate output and optional background processing."""
    try:
        while not self.stop_event.is_set() and not self.error_event.is_set():
            try:
                frame = await asyncio.wait_for(self.audio_input_queue.get(), timeout=5.0)
                if frame is None:
                    break
                
                # CRITICAL: Always output original frame immediately for timing
                output = AudioOutput([frame], self.request_id)
                await self.output_queue.put(output)
                
                # Optional: Background processing for transcription (non-blocking)
                if hasattr(self.frame_processor, 'process_audio_async'):
                    asyncio.create_task(self._process_audio_for_transcription(frame))
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in audio processing: {e}")
                
    except Exception as e:
        logger.error(f"Error in audio processing loop: {e}")

# ADD: Background transcription processing method
async def _process_audio_for_transcription(self, frame):
    """Process audio in background for transcription without affecting stream timing."""
    try:
        # This can take 100-500ms for transcription but doesn't block main stream
        result = await self.frame_processor.process_audio_async(frame)
        
        # Send transcription data via data channel (not main audio stream)
        if result:
            transcription_data = {
                "type": "audio_transcription",
                "timestamp": frame.timestamp,
                "sample_rate": frame.rate,
                "nb_samples": frame.nb_samples
            }
            
            if isinstance(result, list):
                transcription_data["processed_frames"] = len(result)
            
            await self.publish_data(json.dumps(transcription_data))
            logger.debug(f"Published transcription data for timestamp {frame.timestamp}")
            
    except Exception as e:
        logger.error(f"Background transcription error: {e}")
        # Don't re-raise - background processing shouldn't affect main stream

"""
Key Changes from John's Current Implementation:

1. Line 300 BEFORE: 
   processed_frames = await self.frame_processor.process_audio_async(frame)
   
   Line 300 AFTER:
   output = AudioOutput([frame], self.request_id)
   await self.output_queue.put(output)

2. Audio processing moved to background task (non-blocking)

3. Transcription data published via data channel instead of main stream

This maintains John's excellent architectural improvements while fixing audio timing.
"""
