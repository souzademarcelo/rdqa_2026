if (isTRUE(rmarkdown::pandoc_available("1.12.3"))) {
  rmarkdown::render_site()
} else {
  warning("Pandoc nao encontrado; pulando render_site() e seguindo com sync/push.")
}

# --- sincronizar conteudo local para rdqa_2026 no GitLab ---
source_dir        <- "C:/pgquali/site-qualidade-agua-final"
gitlab_repo_dir   <- "C:/pgquali/qualidade-da-agua"  # clone local do repo GitLab
target_subdir     <- "rdqa_2026"

remote_name       <- "origin"
branch            <- "main"
expected_remote   <- "https://gitlab.ana.gov.br/qualidade-da-agua/qualidade-da-agua.git"

git_user_name     <- "Marcelo Souza"
git_user_email    <- "souzademarcelo@gmail.com"

commit_msg        <- sprintf("atualizacao rdqa_2026 - %s", Sys.Date())
use_force_push    <- FALSE

log_file <- file.path(source_dir, "push_gitlab_log.txt")

if (file.exists(log_file)) file.remove(log_file)

run_cmd <- function(cmd, wd = getwd(), allow_fail = FALSE) {
  cat(sprintf("\n[%s] $ %s\n", wd, cmd), file = log_file, append = TRUE)

  wd_win <- gsub("/", "\\\\", normalizePath(wd, winslash = "\\", mustWork = TRUE))
  full_cmd <- sprintf('cd /d "%s" && %s', wd_win, cmd)
  out <- system2("cmd", c("/c", full_cmd), stdout = TRUE, stderr = TRUE)
  status <- attr(out, "status")
  if (is.null(status)) status <- 0L

  if (length(out) > 0) {
    cat(paste(out, collapse = "\n"), "\n", file = log_file, append = TRUE)
  }
  cat(sprintf("[exit_status] %s\n", status), file = log_file, append = TRUE)

  if (status != 0L && !allow_fail) {
    stop(sprintf(
      "Falha ao executar comando (status %s): %s\nVeja o log: %s",
      status, cmd, log_file
    ))
  }

  invisible(list(output = out, status = status))
}

sync_dir <- function(from, to, exclude = character()) {
  dir.create(to, recursive = TRUE, showWarnings = FALSE)

  # limpa destino (espelho)
  old_items <- list.files(to, all.files = TRUE, no.. = TRUE, full.names = TRUE)
  if (length(old_items) > 0) unlink(old_items, recursive = TRUE, force = TRUE)

  items <- list.files(from, all.files = TRUE, no.. = TRUE, full.names = TRUE)
  keep <- basename(items) %in% exclude
  items <- items[!keep]

  for (item in items) {
    dest <- file.path(to, basename(item))

    if (dir.exists(item)) {
      dir.create(dest, recursive = TRUE, showWarnings = FALSE)
      src_win <- gsub("/", "\\\\", normalizePath(item, winslash = "\\", mustWork = TRUE))
      dst_win <- gsub("/", "\\\\", normalizePath(dest, winslash = "\\", mustWork = FALSE))

      # xcopy e mais robusto para copiar diretorios inteiros no Windows
      status <- suppressWarnings(
        system(sprintf('xcopy "%s" "%s\\" /E /I /Y /Q /H', src_win, dst_win), intern = FALSE)
      )

      if (!(status %in% c(0, 1))) {
        stop(sprintf("Falha ao copiar diretorio: %s (codigo xcopy: %s)", item, status))
      }
    } else {
      ok <- file.copy(item, dest, recursive = FALSE, copy.mode = TRUE, overwrite = TRUE)
      if (!ok) stop(sprintf("Falha ao copiar arquivo: %s", item))
    }
  }
}

# 1) garantir clone local do GitLab
if (!dir.exists(file.path(gitlab_repo_dir, ".git"))) {
  parent_dir <- dirname(gitlab_repo_dir)
  dir.create(parent_dir, recursive = TRUE, showWarnings = FALSE)
  run_cmd(sprintf('git clone "%s" "%s"', expected_remote, gitlab_repo_dir), wd = parent_dir)
}

# 2) validar remoto e preparar branch
setwd(gitlab_repo_dir)

run_cmd(sprintf('git config user.name "%s"', git_user_name))
run_cmd(sprintf('git config user.email "%s"', git_user_email))

remotes <- run_cmd("git remote -v")$output
origin_line <- remotes[
  grepl(paste0("^", remote_name, "[[:space:]]+"), remotes) &
    grepl("\\(fetch\\)[[:space:]]*$", remotes)
][1]

if (length(origin_line) == 0) stop(sprintf("Remoto '%s' nao encontrado.", remote_name))

origin_url <- sub(
  paste0("^", remote_name, "\\s+(.+)\\s+\\(fetch\\)$"),
  "\\1",
  origin_line
)

if (origin_url != expected_remote) {
  stop(sprintf("Remoto nao confere. Esperado: %s | Encontrado: %s", expected_remote, origin_url))
}

# garante refspec padrao do remote (necessario para criar refs/remotes/origin/*)
fetch_refspec <- run_cmd(sprintf("git config --get-all remote.%s.fetch", remote_name), allow_fail = TRUE)$output
if (length(fetch_refspec) == 0) {
  run_cmd(sprintf(
    "git config --add remote.%s.fetch \"+refs/heads/*:refs/remotes/%s/*\"",
    remote_name, remote_name
  ))
}

run_cmd("git fetch --all --prune")

remote_branch_exists <- length(
  run_cmd(sprintf("git ls-remote --heads %s %s", remote_name, branch))$output
) > 0

if (remote_branch_exists) {
  # limpa alteracoes locais pendentes no clone antes de trocar branch
  run_cmd("git reset --hard", allow_fail = TRUE)
  run_cmd("git clean -fd", allow_fail = TRUE)

  checkout_status <- run_cmd(
    sprintf("git checkout -B %s %s/%s", branch, remote_name, branch),
    allow_fail = TRUE
  )$status

  if (checkout_status != 0L) {
    # corrige HEAD invalido em clones quebrados
    run_cmd(sprintf("git symbolic-ref HEAD refs/heads/%s", branch), allow_fail = TRUE)
    run_cmd(sprintf("git checkout -B %s %s/%s", branch, remote_name, branch))
  }

  run_cmd(sprintf("git branch --set-upstream-to=%s/%s %s", remote_name, branch, branch), allow_fail = TRUE)
  run_cmd(sprintf("git pull --ff-only %s %s", remote_name, branch))
} else {
  run_cmd(sprintf("git checkout --orphan %s", branch), allow_fail = TRUE)
  run_cmd("git rm -rf .", allow_fail = TRUE)
}

# 3) sincronizar pasta local para rdqa_2026 do repo GitLab
exclude_items <- c(
  ".git",
  ".Rproj.user",
  "push_log.txt",
  "push_gitlab_log.txt",
  "rdqa_2026-main.zip",
  "site-qualidade-agua-final.zip"
)

dest_dir <- file.path(gitlab_repo_dir, target_subdir)
sync_dir(source_dir, dest_dir, exclude = exclude_items)

# 4) commit e push apenas do subdiretorio
run_cmd(sprintf('git add "%s"', target_subdir))
status <- run_cmd(sprintf('git status --porcelain -- "%s"', target_subdir))$output
if (length(status) == 0) stop("Sem alteracoes para commit em rdqa_2026. Abortado.")

run_cmd(sprintf('git commit -m "%s"', commit_msg))

if (use_force_push) {
  run_cmd(sprintf("git push --force %s %s", remote_name, branch))
} else {
  run_cmd(sprintf("git push %s %s", remote_name, branch))
}

message("Concluido. Log em: ", log_file)
