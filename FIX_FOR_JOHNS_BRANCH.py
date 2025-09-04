"""
EXACT FIX for John's current feat/frame-skipper branch

The issue: Line 300 in John's client.py still blocks audio output on processing
The fix: Immediate output + optional background processing
"""

# JOHN'S CURRENT APPROACH (BROKEN):
async def _process_audio_frames_johns_way(self):
    """Process audio frames without skipping."""
    while not self.stop_event.is_set():
        frame = await self.audio_input_queue.get()
        if frame is None:
            break
            
        # THE PROBLEM: This line blocks audio output
        processed_frames = await self.frame_processor.process_audio_async(frame)  # ← BLOCKS FOR 100-500ms
        if processed_frames:
            output = AudioOutput(processed_frames, self.request_id)  # ← DELAYED OUTPUT
            await self.output_queue.put(output)

# FIXED APPROACH (IMMEDIATE OUTPUT):
async def _process_audio_frames_fixed(self):
    """Process audio frames with immediate output principle."""
    while not self.stop_event.is_set():
        frame = await self.audio_input_queue.get()
        if frame is None:
            break
            
        # PRINCIPLE: ALWAYS output original frame immediately for timing
        output = AudioOutput([frame], self.request_id)  # ← IMMEDIATE, original frame
        await self.output_queue.put(output)
        
        # OPTIONAL: Background processing for transcription (doesn't affect timing)
        if hasattr(self.frame_processor, 'process_audio_async') and self.enable_audio_transcription:
            asyncio.create_task(self._process_audio_for_transcription(frame))  # ← NON-BLOCKING

async def _process_audio_for_transcription(self, frame):
    """Background transcription processing."""
    try:
        # This can take 100-500ms, but doesn't affect main audio stream
        transcription_result = await self.frame_processor.process_audio_async(frame)
        
        # Send transcription data via data channel (not main audio stream)
        if transcription_result:
            await self.publish_data(json.dumps({
                "type": "transcription",
                "timestamp": frame.timestamp,
                "data": transcription_result
            }))
    except Exception as e:
        logger.error(f"Background transcription error: {e}")
        # Don't re-raise - this is background work

"""
THE KEY INSIGHT:

Audio for ENCODING (timing) != Audio for PROCESSING (transcription)

- Encoding needs original frames IMMEDIATELY
- Processing can happen in background
- These are separate concerns and should be handled separately
"""
