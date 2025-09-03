# Dual-Path Audio Processing

## Overview

This implementation introduces **dual-path audio processing** to solve the audio distortion issue while enabling real-time transcription capabilities. The solution is inspired by the ai-runner architecture and designed for use with transcription services like project-transcript.

## The Problem

The original feat/frame-skipper branch caused audio distortion because:
1. Audio frames were processed through async queues
2. Processing delays caused timestamp misalignment  
3. Audio synchronization attempted to fix timestamps after the damage was done

## The Solution: Dual-Path Architecture

### Path 1: Immediate Audio Passthrough (Timing Preservation)
- Audio frames are immediately sent to output queue
- Original timestamps preserved
- Zero processing delay for audio timing
- Follows proven ai-runner pattern

### Path 2: Background Transcription Processing  
- Audio frames copied and processed asynchronously
- Transcription results published via data channel
- Does not block main audio/video timing
- Enables real-time transcription without A/V sync issues

## Architecture Diagram

```
Audio Frame Ingress
        |
        v
    [Dual Path Split]
    /             \
   /               \
  v                 v
Path 1:           Path 2:
Immediate         Background
Output            Transcription
(timing)          (data)
  |                 |
  v                 v
Encoder           Data Channel
                  (transcription)
```

## Usage

### Enable Dual-Path Processing

```python
from pytrickle import StreamProcessor
from pytrickle.frame_skipper import FrameSkipConfig

# Define audio processor for transcription
async def audio_transcription_processor(frame: AudioFrame) -> list[AudioFrame]:
    # Your transcription logic here
    # This runs in background without affecting timing
    transcription_text = await transcribe_audio(frame)
    # Return processed frames or metadata
    return [frame]

processor = StreamProcessor(
    video_processor=your_video_processor,
    audio_processor=audio_transcription_processor,  # Transcription processing
    enable_audio_transcription=True,  # Enable dual-path
    frame_skip_config=FrameSkipConfig(),  # Optional frame skipping
)
```

### Client-Level Configuration

```python
from pytrickle import TrickleClient

client = TrickleClient(
    protocol=protocol,
    frame_processor=frame_processor,
    enable_audio_transcription=True,  # Enable background audio processing
    frame_skip_config=frame_skip_config
)
```

## Key Benefits

1. **Zero Audio Distortion**: Original frames maintain perfect timing
2. **Real-Time Transcription**: Background processing enables transcription without blocking
3. **A/V Sync Preservation**: Audio and video streams remain synchronized
4. **Performance**: No processing delays affect audio timing
5. **Backwards Compatible**: Works with existing processors that don't use transcription

## Integration with project-transcript

This architecture enables seamless integration with project-transcript:

1. **Audio frames** go directly to encoder (no distortion)
2. **Transcription data** flows through data channel
3. **SRT subtitles** generated from transcription data
4. **Video timing** unaffected by transcription processing

## Monitoring

Use the statistics API to monitor dual-path processing:

```python
stats = client.get_statistics()
print(f"Audio transcription enabled: {stats['audio_transcription_enabled']}")
print(f"Frame skipping enabled: {stats['frame_skipper_enabled']}")
```

## Migration from Previous Implementation

### Before (Problematic)
```python
# Audio processed through async pipeline - CAUSED DISTORTION
processed_frames = await frame_processor.process_audio_async(frame)
await output_queue.put(AudioOutput(processed_frames, request_id))
```

### After (Dual-Path)
```python
# Path 1: Immediate output for timing
await output_queue.put(AudioOutput([frame], request_id))

# Path 2: Background transcription (if enabled)
if enable_audio_transcription:
    asyncio.create_task(process_audio_for_transcription(frame))
```

## Testing

Run the dual-path test to verify functionality:

```bash
PYTHONPATH=/path/to/pytrickle python tests/test_dual_path_audio.py
```

The test verifies:
- Audio frames output immediately (< 1ms)
- Transcription processing happens in background (200ms+ delay)  
- No timing distortion occurs
- All frames processed correctly
