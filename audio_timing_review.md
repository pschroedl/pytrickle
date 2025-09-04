# Audio Timing Review - feat/frame-skipper

## Issue Summary

John's feat/frame-skipper implementation has excellent architectural improvements but contains a critical audio timing flaw that causes distortion when audio processing is enabled for transcription use cases.

## Core Problem

**File**: `pytrickle/client.py:300`
```python
processed_frames = await self.frame_processor.process_audio_async(frame)
```

This line blocks audio output until processing completes, causing 100-500ms delays for transcription, breaking A/V sync.

## AI-Runner Reference Pattern

**File**: `ai-runner/runner/app/live/streamer/process.py:221`
```python
elif isinstance(input_frame, AudioFrame):
    self._try_queue_put(self.output_queue, AudioOutput([input_frame], self.request_id))
```

Audio frames go directly to output with zero delay, maintaining perfect timing.

## Architectural Improvements (Keep)

- Separate video/audio input queues
- Video-only frame skipping
- Removed broken monotonic audio correction
- Enhanced FPS detection

## Critical Principle

**Audio frames must be sent to output immediately, regardless of whether audio processing is happening.**

For transcription use cases:
- Audio for encoding (timing) != Audio for processing (transcription)
- These are separate concerns requiring different handling

## Solution

Implement dual-path processing:
1. Audio frames go immediately to output queue (timing preservation)
2. Audio frames copied for background processing (transcription via data channel)

## Impact on Project-Transcript

Current implementation prevents proper transcription integration due to audio timing distortion.
Fixed implementation enables real-time transcription without A/V sync issues.
