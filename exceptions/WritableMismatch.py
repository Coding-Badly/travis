
class WritableMismatch(Exception):
    def __init__(self):
        super().__init__('Guessed writable does not match actual writable.')

