# DATACONV

## Migration
- Go to main project directory
  ```bash
  cd ./ckanext/mysql2mongodb
  ```
- Run `alembic` command line
  ```bash
  alembic upgrade heads
  ```
## Add new lib
- Uninstall `ckanext-mysql2mongodb`
  ```bash
  python3 -m pip uninstall ckanext-mysql2mongodb
  ```
- Use poetry to add new library
  ```bash
  poetry add <new library>
  ```
- Reinstall ckanext-mysql2mongodb
  ```bash
  python3 setup.py develop
  ```
