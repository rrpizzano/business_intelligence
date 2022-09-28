import importlib

foobar = importlib.import_module("folder-with-dashes.file_with_functions")
foobar.print_input('Hola')
foobar.print_input('Chau')
