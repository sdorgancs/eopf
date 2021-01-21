# EOPF Proof of Concept

This project is a Proof of Concept to demonstrate the capabilities of the EOPF framework developed by the consortium led by CS GROUP France.

## Development environment

This projects contains a batteries included full web development environment using [code-server](https://github.com/cdr/code-server) and [JupyterLab](https://jupyterlab.readthedocs.io/en/stable/).

### Prerequisites

To start the development environment install first:
- [docker](https://docs.docker.com/engine/install/)
- [docker-compose](https://docs.docker.com/compose/install/)

### Start and stop environment

Running the environment is straightforward, simply type:

```bash
docker-compose up
```

The first run builds the docker image, depending on your network connection speed, this stage can take some time.

Once done, your environment is ready, you can access it on http://localhost/ui/

*Note: the environment do not use HTTPS, modern browser enforcing security automatically add https:// do not miss the http:// par of the URL*

To stop the environment type:

```
docker-compose stop
```

