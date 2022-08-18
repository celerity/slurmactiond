# Installation

## Configuring the Web Server

In order to use a proper SSL-encrypted connection betwen GitHub and slurmactiond, spin up a reverse
proxy like NGINX to handle SSL termination and forward HTTP requests to slurmactiond.

slurmactiond will by default run on port 8020, so if the reverse proxy is running on the same
machine, it should forward to `localhost:8020`.

For NGINX, an example configuration might look like this:

```
server {
    listen       443 ssl http2;
    listen       [::]:443 ssl http2;
    server_name  reverse-proxy.url;
    root         /var/www/reverse-proxy.url;

    ssl_certificate "/etc/letsencrypt/live/reverse-proxy.url/fullchain.pem";
    ssl_certificate_key "/etc/letsencrypt/live/reverse-proxy.url/privkey.pem";
    ssl_session_cache shared:SSL:1m;
    ssl_session_timeout  10m;
    ssl_ciphers HIGH:!aNULL:!MD5;
    ssl_prefer_server_ciphers on;

    # Load configuration files for the default server block.
    include /etc/nginx/default.d/*.conf;

    location /slurmactiond {
        proxy_pass http://localhost:8020/;
    }
}
```

## Adding a GitHub Webhook

1. In your Repository or Organization, navigate to Settings → Webhooks and select Add Webhook.

![Adding a Webhook](doc/images/add-webhook.png)

1. Enter the reverse-proxied URL that points to slurmactiond into the Payload URL field.
2. Select `application/json` as the Content Type.
3. Generate a secure and random Webhook Secret string, and enter it in the Secret field. This will
   be used to sign messages from GitHub to slurmactiond. Keep the the secret around for when we
   enter it into the slurmactiond config file.
4. For the triggering events, manually select only "Worfklow jobs".

![Adding a Webhook](doc/images/add-webhook-form.png)

## Generating a Personal Access Token

1. Choose a user that slurmactiond will impersonate in order to register Actions Runners and
   navigate to its "Settings" -> "Developer Settings".

![Developer Settings](doc/images/dev-settings.png)

2. Select "Personal access token" -> "Generate new token".

![Generating an Access Token](doc/images/generate-access-token.png)

3. Set the token to never expire.
4. Manually select the "repo" and "workflow" scopes, and if slurmactiond is supposed to manage
   runners in an organization instead of a single repository, additionally select "admin:org".

![Generating an Access Token](doc/images/generate-access-token-form.png)

5. Save the generated access token for the slurmactiond config file.

## Installing and Configuring slurmactiond

1. Build the slurmactiond release binary and copy it from `target/release/slurmactiond` to a shared
   filesystem that is accessible by all SLURM nodes.
2. Copy `slurmactiond.example.toml` from this repository to `slurmactiond.toml` into the
   directory where the binary now resides.
3. Complete the configuration:
   - Set `http.secret` to the Secret we used when configuring the Webhook.
   - Set `github.entity` to the organization name or `user/repo` specification where runners
     should be registered.
   - Set `github.api_token` to the Personal Access Token we created above.
   - Select an `action_runner.work_dir` where ephemeral runner installations and files checked out
     from GitHub should reside. Ideally, this directory is not on a shared file system.
   - Create one or more `targets` that will match Workflow job labels to SLURM options. Whenever
     a Workflow job matches all of the `runner_labels`, an ephemeral Actions Runner will be
     scheduled through SLRUM via an `srun` command that receives all the `srun_options` specified
     here.
4. (optional) Register slurmactiond as a systemd service
   - Copy `slurmactiond.example.service` to `/etc/systemd/system/slurmactiond.service` and
     (optionally) adjust the usernames and installation paths within the `.service` file.
   - Start the service via `systemctl start slurmactiond`.
   - (optional) Inspect the logs via `journalctl -u slurmactiond`.
