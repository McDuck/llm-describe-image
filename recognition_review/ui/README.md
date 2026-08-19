# Recognition Review UI

The UI is a static Nginx application that proxies requests to the review API. It presents provisional clusters, permits a cluster or individual record to be labelled, and keeps rejected items in excluded `cluster-*` folders.

Build and run it through [`../README.md`](../README.md). The Nginx configuration proxies the API service named by the parent Compose file.
