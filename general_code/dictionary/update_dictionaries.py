
dict1 = {'foo': 'bar', 'ham': 'eggs'}
dict2 = {'ham': 'spam', 'bar': 'baz'}
dict1.update((k, dict2[k]) for k in dict1.keys() & dict2.keys())

print(dict1)
