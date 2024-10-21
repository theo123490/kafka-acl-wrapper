## Setup


### Environment
update your .env based on .env.example
```bash
export $(cat .env | xargs)
```

### Setup environment with poetry
install poetry with apt

```bash
apt install python3-poetry
```

install packages
```bash
poetry install
```

activate poetry virtual environment
```bash
poetry shell
```

or you can set poetry as default python directly with
```bash
source $(poetry env info --path)/bin/activate	
```

once inside the poetry shell or have move your default python to poetry, the cli tool for this project will be available, run the help flag to make sure it is installed properly
```bash
kafka-acl-wrapper --help
```
