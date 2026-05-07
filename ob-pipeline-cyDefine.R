## ============================================================
## 0. Install dependencies
## ============================================================
install_github_if_missing <- function(package, repo) {
  if (require(package, character.only = TRUE, quietly = TRUE)) {
    return(invisible(TRUE))
  }

  lib_dir <- .libPaths()[[1]]
  lock_dir <- file.path(lib_dir, paste0(".", package, "-install.lock"))

  repeat {
    if (dir.create(lock_dir, showWarnings = FALSE)) {
      break
    }
    if (require(package, character.only = TRUE, quietly = TRUE)) {
      return(invisible(TRUE))
    }
    lock_info <- file.info(lock_dir)
    if (!is.na(lock_info$mtime) && difftime(Sys.time(), lock_info$mtime, units = "mins") > 30) {
      unlink(lock_dir, recursive = TRUE, force = TRUE)
    }
    Sys.sleep(2)
  }
  on.exit(unlink(lock_dir, recursive = TRUE, force = TRUE), add = TRUE)

  if (!require(package, character.only = TRUE, quietly = TRUE)) {
    unlink(file.path(lib_dir, paste0("00LOCK-", package)), recursive = TRUE, force = TRUE)
    remotes::install_github(repo, upgrade = "never")
  }
  if (!require(package, character.only = TRUE, quietly = TRUE)) {
    stop(glue::glue("Failed to install required R package {package}."))
  }

  invisible(TRUE)
}

install_github_if_missing("cyCombine", "biosurf/cyCombine")
install_github_if_missing("cyDefine", "biosurf/cyDefine")

cat("Loading tools...")

library(argparse)
library(glue)
library(data.table)
library(readr)
library(dplyr)
library(utils)
library(jsonlite)
library(cyDefine)



## ============================================================
## 2. Specify paths to your data
##    Wrangle format at location to fit with the tool
## ============================================================

cat("Getting arguments...")
# GET ARGUMENTS
parser <- ArgumentParser(description="FCPS caller")

parser$add_argument('--data.train_matrix',
                    type="character",
                    help='gz-compressed textfile containing the comma-separated data to be clustered.')
parser$add_argument('--data.train_labels',
                    type="character",
                    help='gz-compressed textfile with the true labels.')
parser$add_argument('--data.test_matrix',
                    type="character",
                    help='gz-compressed textfile containing the comma-separated data to be clustered.')
parser$add_argument('--data.label_key',
                    type="character",
                    help='label key metadata path (accepted but unused).')
parser$add_argument('--data.metadata',
                    type="character",
                    dest = "metadata",
                    help='Metadata json.')
parser$add_argument("--output_dir", "-o", dest="output_dir", type="character",
                    help="output directory where files will be saved", default=getwd())
parser$add_argument("--name", "-n", dest="name", type="character", help="name of the dataset")
parser$add_argument("--seed", "-s", dest="seed", type="numeric", help="seed", default = 332)



args <- parser$parse_args()


# FOR TESTING
# Path to zipped data
# dataset_path <- "../cytof_benchmark_test_data"
# train_x_path <- glue("{dataset_path}/data_import.train.matrix.tar.gz")
# train_y_path <- glue("{dataset_path}/data_import.train.labels.tar.gz")
# test_y_path <- glue("{dataset_path}/data_import.test.labels.tar.gz")
# test_x_path <- glue("{dataset_path}/data_import.test.matrices.tar.gz")
# output_dir <- "out"
# seed <- 332

# Prepare a unique temp workspace under output dir to avoid collisions and /tmp limits
output_dir <- args[['output_dir']]
base_tmp <- file.path(
  output_dir,
  paste0("tmp_cydefine_", Sys.getpid(), "_", format(Sys.time(), "%Y%m%d%H%M%S"))
)
dir.create(base_tmp, recursive = TRUE, showWarnings = FALSE)
on.exit(unlink(base_tmp, recursive = TRUE), add = TRUE)




cat("Loading data...")
train_x_path <- args[['data.train_matrix']]
train_y_path <- args[['data.train_labels']]
test_x_path <- args[['data.test_matrix']]
test_x_path <- args[['data.test_matrix']]
metadata_path <- args[['metadata']]
if (is.null(metadata_path) || length(metadata_path) == 0 || is.na(metadata_path)) {
  stop("Missing required --data.metadata path.")
}

seed <- args[['seed']]


# ---------------------------
# Specify paths - unique tmp folder under output dir
# ---------------------------
ExtractTrainX <- file.path(base_tmp, "extract_train_x")
ExtractTrainY <- file.path(base_tmp, "extract_train_y")
ExtractTestX <- file.path(base_tmp, "extract_test_x")


dirs <- c(
  ExtractTrainX,
  ExtractTrainY,
  ExtractTestX
)

for (d in dirs) {
  if (!dir.exists(d)) {
    dir.create(d, recursive = TRUE)
  }
}

extract_archive <- function(path, out_dir) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  utils::untar(path, exdir = out_dir)
}
list_csv_files <- function(path) {
  files <- list.files(path, pattern = "\\.csv$", full.names = TRUE)
  files[order(basename(files))]
}
get_sample_number <- function(file_name, fallback) {
  base <- basename(file_name)
  base <- gsub("\\.csv(\\.gz)?$", "", base)
  m <- regexpr("[0-9]+(?!.*[0-9])", base, perl = TRUE)
  if (m[1] == -1) {
    return(as.character(fallback))
  }
  substr(base, m[1], m[1] + attr(m, "match.length") - 1)
}

read_csv_no_header <- function(path) {
  data.table::fread(path, header = FALSE, data.table = FALSE, showProgress = FALSE)
}

read_label_no_header <- function(path) {
  label_df <- data.table::fread(
    path,
    header = FALSE,
    data.table = FALSE,
    select = 1,
    showProgress = FALSE
  )
  suppressWarnings(as.numeric(label_df[[1]]))
}

read_metadata <- function(path, out_dir) {
  if (grepl("\\.tar(\\.gz)?$|\\.tgz$", path)) {
    dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
    utils::untar(path, exdir = out_dir)
    json_files <- list.files(out_dir, pattern = "\\.json$", full.names = TRUE, recursive = TRUE)
    if (length(json_files) == 0) {
      stop(glue("No metadata JSON found in {path}."))
    }
    return(jsonlite::read_json(json_files[[1]]))
  }

  if (grepl("\\.gz$", path)) {
    con <- gzfile(path, open = "rt")
    on.exit(close(con), add = TRUE)
    return(jsonlite::fromJSON(paste(readLines(con, warn = FALSE), collapse = "\n"), simplifyVector = FALSE))
  }

  jsonlite::read_json(path)
}

metadata_flag <- function(metadata, name, default = FALSE) {
  value <- metadata[[name]]
  if (is.null(value)) {
    value <- metadata$metadata$stratification[[gsub("-", "_", name)]]
  }
  if (is.null(value)) {
    value <- metadata$stages$stratify$stratification[[gsub("-", "_", name)]]
  }
  if (is.null(value)) {
    return(default)
  }
  if (is.logical(value)) {
    return(isTRUE(value))
  }
  if (is.character(value)) {
    return(tolower(value) %in% c("true", "1", "yes"))
  }
  as.logical(value)
}



extract_archive(train_x_path, ExtractTrainX)
extract_archive(train_y_path, ExtractTrainY)
extract_archive(test_x_path, ExtractTestX)
metadata <- read_metadata(metadata_path, file.path(base_tmp, "extract_metadata"))
drop_ungated_training <- metadata_flag(metadata, "drop-ungated-training")
drop_ungated_test <- metadata_flag(metadata, "drop-ungated-test")



train_x_files <- list_csv_files(ExtractTrainX)
train_y_files <- list_csv_files(ExtractTrainY)
test_x_files <- list_csv_files(ExtractTestX)

if (length(train_x_files) == 0) {
  stop("No training matrix CSV files found after extraction.")
}
if (length(train_y_files) == 0) {
  stop("No training label CSV files found after extraction.")
}
if (length(test_x_files) == 0) {
  stop("No test matrix CSV files found after extraction.")
}

x_names <- basename(train_x_files)
y_names <- basename(train_y_files)
x_ids <- vapply(seq_along(x_names), function(i) get_sample_number(x_names[i], i), character(1))
y_ids <- vapply(seq_along(y_names), function(i) get_sample_number(y_names[i], i), character(1))
y_id_counts <- table(y_ids)
use_id_match <- all(x_ids %in% y_ids) && all(y_id_counts == 1)

if (!use_id_match && length(train_x_files) != length(train_y_files)) {
  stop(glue(
    "Training X/Y file count mismatch and sample-id matching failed: x={length(train_x_files)} y={length(train_y_files)}"
  ))
}


marker_count <- NULL
train_feature_chunks <- vector("list", length(train_x_files))
train_label_chunks <- vector("list", length(train_x_files))
for (i in seq_along(train_x_files)) {
  x_file <- train_x_files[[i]]
  y_file <- if (use_id_match) {
    matched <- match(x_ids[i], y_ids)
    if (is.na(matched)) {
      stop(glue("Missing labels for training sample id {x_ids[i]} ({basename(x_file)})"))
    }
    train_y_files[[matched]]
  } else {
    train_y_files[[i]]
  }

  x <- read_csv_no_header(x_file)
  y_num <- read_label_no_header(y_file)

  if (nrow(x) != length(y_num)) {
    stop(glue(
      "Length mismatch for {basename(x_file)} and {basename(y_file)}: data={nrow(x)} labels={length(y_num)}"
    ))
  }

  x_matrix <- as.matrix(x)
  valid_rows <- rowSums(!is.finite(x_matrix)) == 0

  unlabeled_mask <- is.na(y_num) | y_num == 0
  keep <- valid_rows & !unlabeled_mask

  if (!any(keep)) {
    stop(glue("Training sample {basename(x_file)} has no valid labeled rows after filtering."))
  }

  x_clean <- as.data.frame(x_matrix[keep, , drop = FALSE])
  y_clean <- data.frame(V1 = y_num[keep])

  if (is.null(marker_count)) {
    marker_count <- ncol(x_clean)
  } else if (ncol(x_clean) != marker_count) {
    stop(glue(
      "Training marker count mismatch: expected {marker_count}, got {ncol(x_clean)} for {basename(x_file)}"
    ))
  }

  train_feature_chunks[[i]] <- x_clean
  train_label_chunks[[i]] <- y_clean[[1]]
}

if (is.null(marker_count) || marker_count < 1) {
  stop("No valid training markers found after filtering.")
}

RelevantMarkers <- seq_len(marker_count)
training_data <- do.call(rbind, train_feature_chunks)
training_labels <- unlist(train_label_chunks, use.names = FALSE)



## ============================================================
## 3. Run cyDefine
## ============================================================

markers <- colnames(x_clean)[RelevantMarkers]

# Load and transform test data
test_data <- lapply(test_x_files, FUN = read_csv_no_header)
nrows <- sapply(test_data, FUN = nrow)
test_data <- do.call(rbind, test_data)

marker_medians <- apply(as.matrix(training_data[, markers, drop = FALSE]), 2, median, na.rm = TRUE)
marker_medians[!is.finite(marker_medians)] <- 0
for (marker in markers) {
  values <- test_data[[marker]]
  invalid <- !is.finite(values)
  if (any(invalid)) {
    values[invalid] <- marker_medians[[marker]]
    test_data[[marker]] <- values
  }
}


# Transform data
# if (max(training_data[, markers]) > 100) {
#   cofactor <- ifelse(min(training_data[, markers]) < -100, 150, 5)
#   training_data <- transform_asinh(training_data, markers = markers, cofactor = cofactor)
#   test_data <- transform_asinh(test_data, markers = markers, cofactor = cofactor)
# }

training_data$celltype <- training_labels

classified <- cyDefine(
  reference = training_data,
  query = test_data,
  markers = markers,
  num.threads = 1,
  mtry = ceiling(length(markers)/3),
  num.trees = 500,
  batch_correct = TRUE,
  xdim = 6, ydim = 6,
  identify_unassigned = !drop_ungated_test,
  train_on_unassigned = !drop_ungated_training,
  seed = seed,
  verbose = TRUE
)

# FOR TESTING
# ExtractTestY <- file.path(base_tmp, "extract_test_y")
# extract_archive(test_y_path, ExtractTestY)
# test_y_files <- list_csv_files(ExtractTestY)
#
# test_y <- lapply(test_y_files, FUN = read_csv_no_header)
# test_y <- do.call(rbind, test_y)
#
# aricode::ARI(classified$query$model_prediction, test_y$V1)


## ============================================================
## 4. Export labels
## ============================================================

prediction_files <- test_x_files
if (length(prediction_files) == 0) {
  stop("No test CSV files found for prediction.")
}

prediction_column <- if (!drop_ungated_test && "predicted_celltype" %in% names(classified$query)) {
  "predicted_celltype"
} else {
  "model_prediction"
}
if (!prediction_column %in% names(classified$query)) {
  stop(glue("cyDefine output is missing prediction column {prediction_column}."))
}
predictions <- classified$query[[prediction_column]]
if (length(predictions) != sum(nrows)) {
  stop(glue("Prediction count mismatch: predictions={length(predictions)} test_rows={sum(nrows)}"))
}


# Split predictions based on the file nrows
file_groups <- rep(seq_along(nrows), nrows)
predictions <- split(predictions, file_groups)

tmp_dir <- file.path(base_tmp, "predictions_tmp")
dir.create(tmp_dir, recursive = TRUE, showWarnings = FALSE)
csv_files <- character(length(prediction_files))

for (i in seq_along(prediction_files)) {
  prediction_file <- prediction_files[[i]]

  csv_file <- file.path(tmp_dir, basename(prediction_file))
  data.table::fwrite(as.data.frame(predictions[[i]]), file = csv_file, col.names = FALSE, quote = FALSE)
  csv_files[i] <- csv_file
}

# Create tar.gz archive of all CSVs
name <- args[['name']]
tar(tarfile = glue("{output_dir}/{name}_predicted_labels.tar.gz"), files = csv_files, compression = "gzip", tar = "internal")


# Temporary workspace is cleaned on exit.
