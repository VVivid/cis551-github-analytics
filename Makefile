PYTHON ?= python3
TERRAFORM ?= terraform
REGION ?=
HOURS ?= all
DEMO_HOUR ?= 23
EXPECTED_HOURS ?= 24

.PHONY: install test fmt validate init plan apply views ingest etl prepare-demo finish-demo verify repeat-check failure-check plan-destroy destroy

install:
	$(PYTHON) -m pip install -r requirements.txt

test:
	$(PYTHON) -m unittest discover -s tests -v

fmt:
	$(TERRAFORM) fmt -recursive terraform

validate:
	$(TERRAFORM) -chdir=terraform validate

init:
	@test -n "$(BACKEND)" || (echo 'Set BACKEND=/absolute/private/backend.hcl'; exit 1)
	$(TERRAFORM) -chdir=terraform init -backend-config="$(BACKEND)"

plan:
	@test -n "$(TFVARS)" || (echo 'Set TFVARS=/absolute/private/project.tfvars'; exit 1)
	$(TERRAFORM) -chdir=terraform plan -var-file="$(TFVARS)"

apply:
	@test -n "$(TFVARS)" || (echo 'Set TFVARS=/absolute/private/project.tfvars'; exit 1)
	$(TERRAFORM) -chdir=terraform apply -var-file="$(TFVARS)"

views ingest etl prepare-demo finish-demo verify repeat-check failure-check:
	TERRAFORM="$(TERRAFORM)" $(PYTHON) scripts/run.py $@ --region "$(REGION)" --hours "$(HOURS)" --demo-hour "$(DEMO_HOUR)" --expected-hours "$(EXPECTED_HOURS)"

plan-destroy:
	@test -n "$(TFVARS)" || (echo 'Set TFVARS=/absolute/private/project.tfvars'; exit 1)
	$(TERRAFORM) -chdir=terraform plan -destroy -var-file="$(TFVARS)" -var='allow_bucket_destroy=true'

destroy:
	@test -n "$(TFVARS)" || (echo 'Set TFVARS=/absolute/private/project.tfvars'; exit 1)
	# The AWS provider must save force_destroy in state before emptying the bucket.
	$(TERRAFORM) -chdir=terraform apply -var-file="$(TFVARS)" -var='allow_bucket_destroy=true'
	$(TERRAFORM) -chdir=terraform destroy -var-file="$(TFVARS)" -var='allow_bucket_destroy=true'
