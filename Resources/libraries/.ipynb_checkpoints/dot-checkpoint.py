from subprocess import Popen as Bash
from subprocess import PIPE as PIPE
from IPython.display import SVG

def dot_image(s):
	with open('source.dot', 'w') as f:
		f.write(s)		
	process = Bash(['dot', '-Tsvg', 'source.dot'], stdout=PIPE)
	output, error = process.communicate()
	return SVG(output)