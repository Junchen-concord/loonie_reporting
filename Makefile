.PHONY: venv install run-dashboard

venv:
	python -m venv .venv

install:
	python -m pip install --upgrade pip
	pip install -r requirements.txt

run-dashboard:
	streamlit run python/dashboard_kpis.py


