
# Directions API

The Google API key is stored in 'keyring'.  This makes it available in notebooks and scripts without being visible in 
source files/git etc.  

To set the key run:

```bash
python -m keyring set logicalgenetics google
```

To get the key value in code use this snippet:

```python
import keyring
keyring.get_password('logicalgenetics', 'google')
```