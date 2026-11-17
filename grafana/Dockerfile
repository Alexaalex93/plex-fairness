# Grafana with the plexfair datasource and the dashboard already inside
FROM grafana/grafana-oss:11.6.0
COPY provisioning /etc/grafana/provisioning
COPY dashboards /etc/grafana/dashboards
# open straight into the Plex dashboard
ENV GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/etc/grafana/dashboards/plex-fairness.json
