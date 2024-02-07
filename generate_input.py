#chatgpt generated
import numpy as np

# Generate random 16-byte integers
num_integers = 2**24
integers = np.random.bytes(16 * num_integers)

# Reshape into a 2D array of 16-byte integers
integers_array = np.frombuffer(integers, dtype=np.uint8)
integers_array = integers_array.reshape(num_integers, 16)

# Save the integers into a file
filename = 'random_integers.bin'
integers_array.tofile(filename)
