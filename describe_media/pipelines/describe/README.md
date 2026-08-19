# Describe pipeline

`describe` is the production media graph. It discovers media, extracts video frames, records metadata, resizes the LLM input when required, runs recognition on the original image, calls the LLM, and enhances the description with nearby context.

Run `describe-media describe <input-dir> <output-dir>`. Geolocation is optional and does not hold up captioning; metadata, resize, and recognition are required before the LLM runs. Failed descriptions are recorded beside the affected media and can be retried with `--retry-failed`.
