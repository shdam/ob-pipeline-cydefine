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
parser$add_argument("--prediction-chunk-size",
                    dest="prediction_chunk_size",
                    type="integer",
                    help="number of query rows to classify per ranger prediction call",
                    default = as.integer(Sys.getenv("CYDEFINE_PREDICTION_CHUNK_SIZE", "50000")))
parser$add_argument("--prediction-threads",
                    dest="prediction_threads",
                    type="integer",
                    help="number of ranger threads to use for each prediction chunk",
                    default = as.integer(Sys.getenv("CYDEFINE_PREDICTION_THREADS", "8")))



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
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
output_dir <- normalizePath(output_dir, mustWork = TRUE)
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
prediction_chunk_size <- args[['prediction_chunk_size']]
if (is.null(prediction_chunk_size) || is.na(prediction_chunk_size) || prediction_chunk_size < 1) {
  stop("--prediction-chunk-size must be a positive integer.")
}
prediction_threads <- args[['prediction_threads']]
if (is.null(prediction_threads) || is.na(prediction_threads) || prediction_threads < 1) {
  stop("--prediction-threads must be a positive integer.")
}


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
ungated_label <- "unassigned"



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

  ungated_mask <- is.na(y_num) | y_num == 0
  keep <- if (drop_ungated_training) {
    valid_rows & !ungated_mask
  } else {
    valid_rows
  }

  if (!any(keep)) {
    stop(glue("Training sample {basename(x_file)} has no valid labeled rows after filtering."))
  }

  x_clean <- as.data.frame(x_matrix[keep, , drop = FALSE])
  y_clean <- as.character(y_num[keep])
  y_clean[is.na(y_num[keep]) | y_num[keep] == 0] <- ungated_label

  if (is.null(marker_count)) {
    marker_count <- ncol(x_clean)
  } else if (ncol(x_clean) != marker_count) {
    stop(glue(
      "Training marker count mismatch: expected {marker_count}, got {ncol(x_clean)} for {basename(x_file)}"
    ))
  }

  train_feature_chunks[[i]] <- x_clean
  train_label_chunks[[i]] <- y_clean
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

cat("Batch correcting with cyDefine...\n")
t <- system.time({
  corrected <- get("batch_correct", asNamespace("cyDefine"))(
    reference = training_data,
    query = test_data,
    markers = markers,
    xdim = 6, ydim = 6,
    seed = seed,
    verbose = TRUE,
    num.threads = 1
  )
})
message("Batch correction took ", round(t[[3]], 2), " seconds")
reference <- corrected$reference
query <- corrected$query
rm(corrected, test_data)
gc()

train_cydefine_model <- function(reference, markers, mtry, num.trees, seed, num.threads = 1,
                                 filter_unassigned = TRUE, unassigned_name = ungated_label,
                                 verbose = TRUE) {
  if (filter_unassigned) {
    reference <- dplyr::filter(reference, celltype != !!unassigned_name)
  }
  if (nrow(reference) == 0) {
    stop("No reference rows available for cyDefine model training.")
  }

  if (verbose) {
    message("Training random forest model using ", num.threads, " threads")
  }
  set.seed(seed)
  subset <- dplyr::slice_sample(dplyr::group_by(reference, celltype), n = 2000)
  model_weights <- dplyr::pull(
    dplyr::reframe(dplyr::group_by(reference, celltype), weight = dplyr::n() / nrow(reference)),
    weight
  )

  t <- system.time({
    rf_model <- ranger::ranger(
      y = as.factor(subset$celltype),
      x = subset[, markers],
      num.trees = num.trees,
      mtry = mtry,
      importance = "impurity",
      write.forest = TRUE,
      probability = TRUE,
      min.node.size = 1,
      replace = TRUE,
      sample.fraction = 1,
      class.weights = model_weights,
      splitrule = "gini",
      num.threads = num.threads,
      seed = seed,
      save.memory = FALSE,
      verbose = verbose,
      oob.error = FALSE
    )
  })
  if (verbose) {
    message("Model training took ", round(t[[3]], 2), " seconds")
  }
  rf_model
}

predict_cydefine_chunks <- function(rf_model, query, markers, chunk_size, prediction_threads, verbose = TRUE) {
  n <- nrow(query)
  predictions <- character(n)
  max_probs <- numeric(n)
  if (n == 0) {
    return(list(predictions = predictions, max_probs = max_probs))
  }
  starts <- seq.int(1, n, by = chunk_size)
  if (verbose) {
    message(
      "Predicting in ", length(starts), " chunk(s) of up to ", chunk_size,
      " rows using ", prediction_threads, " ranger thread(s)"
    )
  }
  for (chunk_index in seq_along(starts)) {
    start <- starts[[chunk_index]]
    end <- min(start + chunk_size - 1, n)
    if (verbose && (chunk_index == 1 || chunk_index %% 25 == 0 || chunk_index == length(starts))) {
      message(
        "Predicting chunk ", chunk_index, "/", length(starts),
        " (rows ", start, "-", end, ")"
      )
    }
    pred <- stats::predict(
      object = rf_model,
      data = query[start:end, markers, drop = FALSE],
      num.threads = prediction_threads
    )$predictions
    if (is.null(dim(pred))) {
      predictions[start:end] <- as.character(pred)
      max_probs[start:end] <- NA_real_
    } else {
      predictions[start:end] <- colnames(pred)[max.col(pred, ties.method = "first")]
      max_probs[start:end] <- apply(pred, 1, max)
    }
    rm(pred)
    gc(FALSE)
  }
  list(predictions = predictions, max_probs = max_probs)
}

mtry <- ceiling(length(markers) / 3)
rf_model <- train_cydefine_model(
  reference = reference,
  markers = markers,
  mtry = mtry,
  num.trees = 500,
  seed = seed,
  filter_unassigned = TRUE,
  verbose = TRUE
)

cat("Predicting with chunked cyDefine classifier...\n")
t <- system.time({
  prediction_result <- predict_cydefine_chunks(
    rf_model = rf_model,
    query = query,
    markers = markers,
    chunk_size = prediction_chunk_size,
    prediction_threads = prediction_threads,
    verbose = TRUE
  )
})
message("Classification took ", round(t[[3]], 2), " seconds")
predictions <- prediction_result$predictions

if (!drop_ungated_test) {
  if (!drop_ungated_training) {
    message("Identifying unassigned cells per predicted cell type")
    final_predictions <- predictions
    for (popu in unique(predictions)) {
      idx <- which(predictions == popu)
      celltype_ref <- dplyr::filter(reference, celltype == !!popu | celltype == !!ungated_label)
      n_popu <- nrow(dplyr::filter(celltype_ref, celltype == !!popu))
      if (n_popu < 30) {
        warning(
          "Too few cells are available for celltype '", popu,
          "' for modelling, thus no cells predicted to belong to ",
          popu, " will be identified as 'unassigned'"
        )
        next
      }
      unassigned_model <- train_cydefine_model(
        reference = celltype_ref,
        markers = markers,
        mtry = mtry,
        num.trees = 300,
        seed = seed,
        filter_unassigned = FALSE,
        verbose = FALSE
      )
      final_predictions[idx] <- predict_cydefine_chunks(
        rf_model = unassigned_model,
        query = query[idx, , drop = FALSE],
        markers = markers,
        chunk_size = prediction_chunk_size,
        prediction_threads = prediction_threads,
        verbose = FALSE
      )$predictions
      rm(unassigned_model)
      gc(FALSE)
    }
    predictions <- final_predictions
    rm(final_predictions)
  } else {
    predictions[prediction_result$max_probs < 0.8] <- ungated_label
  }
}

rm(rf_model, prediction_result, query, reference)
gc()

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

predictions <- as.character(predictions)
ungated_predictions <- is.na(predictions) | tolower(predictions) %in% c("", "0", "0.0", "na", "nan", "ungated", ungated_label)
predictions[ungated_predictions] <- "0"
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

# Create tar.gz archive of all CSVs with short member names.
name <- args[['name']]
output_tar <- normalizePath(
  file.path(output_dir, paste0(name, "_predicted_labels.tar.gz")),
  mustWork = FALSE
)
dir.create(dirname(output_tar), recursive = TRUE, showWarnings = FALSE)
old_wd <- getwd()
tar_error <- tryCatch(
  {
    setwd(tmp_dir)
    tar(tarfile = output_tar, files = basename(csv_files), compression = "gzip", tar = "internal")
    NULL
  },
  error = function(e) e,
  finally = setwd(old_wd)
)
if (!is.null(tar_error)) {
  stop(tar_error)
}

if (!file.exists(output_tar)) {
  stop(glue("Failed to create prediction archive {output_tar}."))
}

unlink(base_tmp, recursive = TRUE, force = TRUE)
