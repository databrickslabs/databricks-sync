# Contributing

This page can help contributors get started.

## Environment variables

The following configuration attributes can be passed via environment variables:

| Argument | Environment variable |
| --: | --- |
| `--profile` | `AZURE_SOURCE_WORKSPACE` or `AZURE_TARGET_WORKSPACE` |
| `--git-ssh-url` | `GIT_REPO` |
| `--branch` | `MASTER_REVISION` |
| `--revision` | `MASTER_REVISION` |
| `--destroy` | `DESTROY` |
| `--artifact-dir` | `ARTIFACT_DIR` |
| `--backup` | `BACKUP_FILE` |

### Using .env file

This project also uses the `python-dotenv` library and calls `load_dotenv` when the application starts. You can also use a `.env` file. Just change `.env.template` to `.env` and replace the values in `.env` with the values for your environment.  Leave the `.env` file in same directory as `.env.template` for `dot_env` library to read it.
