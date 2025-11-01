.PHONY: help setup clean test deploy validate run lint format check-uv

# Default target
.DEFAULT_GOAL := help

# Color output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)Databricks Asset Bundle - my_project$(NC)"
	@echo ""
	@echo "$(GREEN)Available targets:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-15s$(NC) %s\n", $$1, $$2}'

check-uv: ## Check if uv is installed
	@command -v uv >/dev/null 2>&1 || { echo "$(YELLOW)uv is not installed. Please install it: https://docs.astral.sh/uv/getting-started/installation/$(NC)"; exit 1; }

setup: check-uv ## Install dependencies using uv
	@echo "$(BLUE)Installing project dependencies...$(NC)"
	uv sync --dev
	@echo "$(GREEN)Setup complete!$(NC)"

clean: ## Clean cache, build artifacts, and temporary files
	@echo "$(BLUE)Cleaning cache and build artifacts...$(NC)"
	rm -rf build/
	rm -rf dist/
	rm -rf .databricks/
	rm -rf *.egg-info
	rm -rf src/*.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.coverage" -delete
	rm -rf .coverage
	rm -rf htmlcov/
	@echo "$(GREEN)Clean complete!$(NC)"

test: ## Run unit tests with pytest
	@echo "$(BLUE)Running unit tests...$(NC)"
	uv run pytest tests/ -v
	@echo "$(GREEN)Tests complete!$(NC)"

test-coverage: ## Run tests with coverage report
	@echo "$(BLUE)Running tests with coverage...$(NC)"
	uv run pytest tests/ -v --cov=src/my_project --cov-report=html --cov-report=term
	@echo "$(GREEN)Coverage report generated in htmlcov/$(NC)"

validate: ## Validate bundle configuration
	@echo "$(BLUE)Validating bundle configuration...$(NC)"
	databricks bundle validate
	@echo "$(GREEN)Validation complete!$(NC)"

deploy-dev: ## Deploy to development environment
	@echo "$(BLUE)Deploying to development...$(NC)"
	databricks bundle deploy --target dev
	@echo "$(GREEN)Deployment complete!$(NC)"

deploy-prod: ## Deploy to production environment
	@echo "$(BLUE)Deploying to production...$(NC)"
	databricks bundle deploy --target prod
	@echo "$(GREEN)Deployment complete!$(NC)"

run: ## Run the default job
	@echo "$(BLUE)Running bundle job...$(NC)"
	databricks bundle run

build: ## Build Python wheel
	@echo "$(BLUE)Building Python wheel...$(NC)"
	uv build --wheel
	@echo "$(GREEN)Build complete!$(NC)"

install-dev: setup ## Install project in development mode
	@echo "$(BLUE)Installing project in development mode...$(NC)"
	uv pip install -e .
	@echo "$(GREEN)Development install complete!$(NC)"

all: clean setup test validate ## Clean, setup, test, and validate
	@echo "$(GREEN)All tasks complete!$(NC)"
