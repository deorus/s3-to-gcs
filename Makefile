BINARY_NAME := s3-to-gcs
BUILD_DIR := build
GO_FILES := $(wildcard *.go)

# Build
.PHONY: all
all: clean build

.PHONY: build
build: $(GO_FILES)
	@echo "Building $(BINARY_NAME)..."
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) $(GO_FILES)

# Clean
.PHONY: clean
clean:
	@echo "Cleaning up..."
	@rm -rf $(BUILD_DIR)

# Run
.PHONY: run
run: build
	@echo "Running $(BINARY_NAME)..."
	@./$(BUILD_DIR)/$(BINARY_NAME)
