# Contributing

This page can help contributors get started.

## OS

Development so far has only been using Unix/Linux/MacOS. We could use someone looking to begin testing on Windows though.

### Using .env file

This project also uses the `python-dotenv` library and calls `load_dotenv` when the application starts. You can also use a `.env` file. Just change `.env.template` to `.env` and replace the values in `.env` with the values for your environment.  Leave the `.env` file in same directory as `.env.template` for `dot_env` library to read it.
