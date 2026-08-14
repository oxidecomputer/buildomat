# Getting started for developing buildomat

Buildomat needs to run on an illumos system


- `mkdir buildomat_stuff`

- `cd buildomat_stuff`

- `mkdir data`

- `touch data/data.sqlite3`

- create a sample `server.toml` file

```
[admin]
token = "<ADMIN TOKEN>"
hold = false

[general]
baseurl = "NOTUSED"

[storage]
access_key_id = "YOUR VALUE"
secret_access_key = "YOUR VALUE"
bucket = "YOUR VALUE"
prefix = "YOUR VALUE"
region = "YOUR VALUE"

[job]
max_runtime = <something reasonable>

[sqlite]
````

- run `buildomat-server -f server.toml`. This should start a server process.

- Create a `user.toml` file (this can also live in `~/.config/buildomat/config.toml`
```
[profile.default]
url = "http://127.0.0.1:9979"
secret = "<ADD TOKEN>"
admin = "<ADD TOKEN>"
```
- run `buildomat -p user.toml admin user create admin`. This will spit out an `ADMIN_TOKEN`
- Edit `user.toml` to add the token to both `secret` and `admin` (you can create another user later if you so desire!)

- Edit `server.toml` to add the token to the `admin` line

- Create a factory `buildomat admin factory create factory_name` this will
spit out a `FACTORY_TOKEN` which needs to go in a factory TOML file

- Create a target `buildomat admin target create -d "something here" targetname`
this will spit out a `TARGET_TOKEN` which will go in the factory TOML file

This should be enough to run the factory of your choosing and run
`buildomat job run -c your_script.sh -t yourtarget -n jobname`

nginx is available on illumos and easy to setup as a reverse proxy to
redirect requests to your `buildomat-server`

```
        location / {
            proxy_pass   http://127.0.0.1:9979;
        }
```
