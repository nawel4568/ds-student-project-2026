CONTAINER_IMAGE := texlive/texlive:latest
REPORT_DIR      := report
BUILD_DIR       := build
OUTPUT_PDF      := report.pdf
DOCKER_CMD      := docker run --rm --user $$(id -u):$$(id -g) -v "$(PWD):/workdir" -w /workdir $(CONTAINER_IMAGE) $(MAKE)

.PHONY: report-build clean report

report-build:
	cd $(REPORT_DIR) && latexmk -pdf -interaction=nonstopmode -output-directory=$(BUILD_DIR) main
	cp $(REPORT_DIR)/$(BUILD_DIR)/main.pdf $(OUTPUT_PDF)

clean:
	$(RM) -r $(REPORT_DIR)/$(BUILD_DIR) $(OUTPUT_PDF)

report:
	$(DOCKER_CMD) report-build
