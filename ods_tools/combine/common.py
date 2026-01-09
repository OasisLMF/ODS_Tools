import numpy as np
import numba as nb
import os

oasis_int = np.dtype(os.environ.get('OASIS_INT', 'i4'))
nb_oasis_int = nb.from_dtype(oasis_int)
