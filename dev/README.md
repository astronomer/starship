# starship-dev

This project allows you to either test your local version of starship in a local astro dev environment or deploy it to a remote deployment.

## Commands

The following commands are available to manage a local astro dev environment or deploy to a deployment in Astro.

```bash
just bash *ARGS    # run `astro dev bash [ARGS]` with the webserver pre-selected
just default
just deploy *ARGS  # run `astro dev deploy [ARGS]` with image suitable for deployments in astro
just kill          # run `astro dev kill`
just logs *ARGS    # run `astro dev logs [ARGS]` with the webserver pre-selected
just ps            # run `astro dev ps`
just reload        # restart local webserver with updated frontend assets and plugin code
just restart *ARGS # run `astro dev restart [ARGS]` with image suitable for local development
just start *ARGS   # run `astro dev start [ARGS]` with image suitable for local development
just stop          # run `astro dev stop`
```
