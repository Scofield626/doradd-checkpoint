infer run \
	--reactive \
  --compilation-database app/build/compile_commands.json \
  --skip-analysis-in-path external/ \
	--keep-going
