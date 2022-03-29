# DATACONV

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
