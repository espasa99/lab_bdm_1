import urllib
url = 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=416c6da9-f546-48f4-858d-836c25fa2ed3&limit=5&q=title:jones'
fileobj = urllib.urlopen(url)
print fileobj.read()