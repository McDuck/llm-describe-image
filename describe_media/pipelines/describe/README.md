# Describe pipeline

`describe` is the production media graph. It discovers media, extracts video frames, optionally transcribes the audio interval represented by each frame, records metadata, resizes the LLM input when required, runs recognition on the original image, calls the LLM, and enhances the description with nearby context. After all frame captions are available, it also creates a text-only source-video description from the combined frame captions and whole-video transcript.

Run `describe-media describe <input-dir> <output-dir>`. Geolocation is optional and does not hold up captioning; metadata, resize, and recognition are required before the LLM runs. Failed descriptions are recorded beside the affected media and can be retried with `--retry-failed`.
