# External GPU host

This directory contains services deployed independently from Describe Media. The recognition worker is self-contained: a GPU host can install its requirements and start it without installing the Describe Media application.

- [`recognition/README.md`](recognition/README.md) documents the authenticated InsightFace worker, Windows DirectML setup, and reverse SSH tunnel.
- [`openai/koboldcpp/README.md`](openai/koboldcpp/README.md) documents a Windows KoboldCpp server used through Describe Media's OpenAI-compatible backend.
