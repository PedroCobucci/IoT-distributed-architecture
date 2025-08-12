FROM grafana/grafana-oss:11.1.0


COPY ./assets/fav32.png /usr/share/grafana/public/img/fav32.png
COPY ./assets/fav32.png /usr/share/grafana/public/img/apple-touch-icon.png
COPY ./assets/logo.svg /usr/share/grafana/public/img/grafana_icon.svg

ENV GF_INSTALL_PLUGINS=frser-sqlite-datasource
