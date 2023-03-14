# Installs additional required R packages, not already included in the
# Tidyverse Docker image.

# Run-time
install.packages('argparse')
install.packages('ape')
install.packages('config')
install.packages('countrycode')
install.packages('phangorn')
install.packages('RPostgres')

# Development
install.packages('languageserver')
install.packages(
	file.path(
		'https://github.com/ManuelHentschel',
		'VSCode-R-Debugger',
		'releases',
		'download',
		'v0.5.2',
		'vscDebugger_0.5.2.tar.gz'
	)
)
