Making a New Release
====================

 - create and upload a new github tag
 - version++ for 'version' and 'download url'

```python
 setup(
    # ...
    version='0.1.25',
    # ...
    download_url='https://github.com/joshbrooks/django_viewedmodels/archive/0.1.25.tar.gz',
    )
```

 - add a github tag == version
 - run these commands:

```bash
python setup.py bdist_wheel --universal
python setup.py sdist
twine upload dist/*
```