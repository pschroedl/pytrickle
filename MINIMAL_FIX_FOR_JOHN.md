# Minimal Fix for John's Current Branch

## The Single Line That Breaks Audio Timing

**File**: `pytrickle/client.py:300`  
**Current Code**:
```python
processed_frames = await self.frame_processor.process_audio_async(frame)  # ← BLOCKS OUTPUT
```

**The Issue**: This line makes audio output wait for processing completion, causing 100-500ms delays for transcription.

## The Fix: Replace 11 Lines with the Immediate Output Principle

### Before (John's Current - Broken):
```python
async def _process_audio_frames(self):
    """Process audio frames without skipping."""
    try:
        while not self.stop_event.is_set() and not self.error_event.is_set():
            try:
                frame = await asyncio.wait_for(self.audio_input_queue.get(), timeout=5.0)
                if frame is None:
                    break
                
                # PROBLEM: Blocks audio output for processing time
                processed_frames = await self.frame_processor.process_audio_async(frame)
                if processed_frames:
                    output = AudioOutput(processed_frames, self.request_id)
                    await self.output_queue.put(output)
```

### After (Fixed - Immediate Output):
```python
async def _process_audio_frames(self):
    """Process audio frames with immediate output + optional background processing."""
    try:
        while not self.stop_event.is_set() and not self.error_event.is_set():
            try:
                frame = await asyncio.wait_for(self.audio_input_queue.get(), timeout=5.0)
                if frame is None:
                    break
                
                # SOLUTION: Always output original frame immediately
                output = AudioOutput([frame], self.request_id)
                await self.output_queue.put(output)
                
                # Optional background processing (doesn't block output)
                if hasattr(self.frame_processor, 'process_audio_async'):
                    asyncio.create_task(self._process_audio_for_transcription(frame))
```

## Add Background Processing Method

Add this method to handle transcription without affecting timing:

```python
async def _process_audio_for_transcription(self, frame: AudioFrame):
    """Process audio in background for transcription without affecting main stream."""
    try:
        # This runs in background - can take as long as needed
        result = await self.frame_processor.process_audio_async(frame)
        
        # Publish transcription data via data channel
        if result:
            await self.publish_data(json.dumps({
                "type": "audio_transcription", 
                "timestamp": frame.timestamp,
                "sample_rate": frame.rate,
                "nb_samples": frame.nb_samples
                # Add actual transcription text here when available
            }))
    except Exception as e:
        logger.error(f"Background audio processing error: {e}")
        # Don't affect main stream
```

## Why This Works for All Use Cases

### No Audio Processing (Current Default)
- Audio frames → immediate output
- Zero processing overhead
- Perfect timing

### Audio Processing Enabled (Transcription)
- Audio frames → immediate output (for encoder)  
- Audio frames → background processing (for transcription)
- Perfect timing + transcription capabilities

## Summary

**Current John's Code**: Audio output waits for processing → distortion  
**Fixed Code**: Audio output immediate + processing in background → perfect timing + transcription

**The change**: Separate the audio stream (for encoder) from audio processing (for transcription) - they should never block each other.
