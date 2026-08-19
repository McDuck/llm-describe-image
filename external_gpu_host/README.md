# External GPU host

This directory contains GPU-side services deployed independently from Describe Media. The shared GPU API keeps face recognition and video-frame extraction together below its `services/` implementation directory while exposing one authenticated endpoint and configuration.

- [`gpu/README.md`](gpu/README.md) documents the authenticated shared GPU API, its services, Windows DirectML setup, and reverse SSH tunnel.
- [`openai/koboldcpp/README.md`](openai/koboldcpp/README.md) documents a Windows KoboldCpp server used through Describe Media's OpenAI-compatible backend.
