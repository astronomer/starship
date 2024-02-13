# Docker Container Tests

This special pytest suite is called from `validation_test.py:test_docker_pytest`.

They don't run unless the `DOCKER_TEST=true` environment variable is set, which that `test_docker_pytest` sets.

Additionally, you can run a single test against an image manually with
```shell
just test-run-container-test <image>
```

## The Tests
Docker containers are spawned in parallel with different images,
- `starship` is volume-mounted into the container (and a unique `./build` directory)
- [./run_container_test.sh](./run_container_test.sh) is called with the image name as an argument
  - The script upgrades pip (sometimes required), installs pytest, and installs starship from the volume mount, exiting unsuccessfully if that didn't succeed
  - it copies a DAG to what it thinks is the `/dag` folder, and exits unsuccessfully if it can't
  - it runs `airflow db init`, and exits unsuccessfully if it can't
  - it runs `airflow webserver --workers 1` and `airflow scheduler`, and looks for `Listening at: http://0.0.0.0:8080`. It times out if that doesn't occur within 300s
  - It tries to import the DAGBag directly in python (this doesn't seem to work, likely due to config stuff, no big, serves as a delay for the scheduler to do the import)
  - it runs the `pytest` suite in the container, and exits unsuccessfully if it doesn't pass

# Troubleshooting
**if you see:**
```shell
error: [Errno 2] No such file or directory: 'build/bdist.linux-aarch64/wheel/astronomer_starship-2.0.0-py3.7.egg-info'
...
ERROR: Failed building wheel for astronomer-starship
```
do a quick `just clean-backend-build` locally.
Sometimes permissions get weird.
The pytest that launches containers should do this ahead of testing
