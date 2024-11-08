install:
	pip install --upgrade pip && pip install -r Requirements.txt

format:
	black *.py
	
lint:
	ruff check *.py

test:
	python3 -m pytest -vv --nbval -cov=mylib -cov=main_file test_*.py *.ipynb
	
all: install format lint test

generate_and_push:
	# Create the markdown file
	python main.py

	# Add, commit, and push the generated files to GitHub
	@if [ -n "$$(git status --porcelain)" ]; then \
		git config --local user.email "action@github.com"; \
		git config --local user.name "GitHub Action"; \
		git add .; \
		git commit -m "Add generated plot and report"; \
		git push; \
	else \
		echo "No changes to commit. Skipping commit and push."; \
	fi
