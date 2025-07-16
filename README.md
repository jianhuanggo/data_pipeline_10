Welcome to aws-lib 👋
aws-lib

Quick Start for aws object

Get aws s3 client object

setup an aws profile named aws_default
aws configure --profile aws_default

in /_config/config.yaml, appropriate corresponding entry for aws profile.  it is defaulted to aws_default.
then using below two lines of code to get obtain s3 client object

```python

from _connect import _connect as _connect_
_object_s3 = _connect_.get_object("awss3")

```


Enjoy!