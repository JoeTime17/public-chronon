#!/bin/bash
# Set up opsagent to expose prometheus metrics for flink and spark jobs
# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

if [[ $(echo "${DATAPROC_IMAGE_VERSION} < 2.2" | bc -l) == 1  ]]; then
  echo "This Dataproc cluster node runs image version ${DATAPROC_IMAGE_VERSION} with pre-installed legacy monitoring agent. Skipping Ops Agent installation."
  exit 0
fi

curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
bash add-google-cloud-ops-agent-repo.sh --also-install

cat <<EOF >> /etc/google-cloud-ops-agent/config.yaml
metrics:
  receivers:
    prometheus_flink:
      type: prometheus
      config:
        scrape_configs:
          - job_name: 'flink'
            scrape_interval: 15s
            metrics_path: /metrics
            static_configs:
              - targets: [
                'localhost:9250',
                'localhost:9251',
                'localhost:9252',
                'localhost:9253',
                'localhost:9254',
                'localhost:9255',
                'localhost:9256',
                'localhost:9257',
                'localhost:9258',
                'localhost:9259',
                'localhost:9260',
                'localhost:9261',
                'localhost:9262',
                'localhost:9263',
                'localhost:9264',
                'localhost:9265',
                'localhost:9266',
                'localhost:9267',
                'localhost:9268',
                'localhost:9269',
                'localhost:9270',
                'localhost:9271',
                'localhost:9272',
                'localhost:9273',
                'localhost:9274',
                'localhost:9275',
                'localhost:9276',
                'localhost:9277',
                'localhost:9278',
                'localhost:9279',
                'localhost:9280',
                'localhost:9281',
                'localhost:9282',
                'localhost:9283',
                'localhost:9284',
                'localhost:9285',
                'localhost:9286',
                'localhost:9287',
                'localhost:9288',
                'localhost:9289',
                'localhost:9290',
                'localhost:9291',
                'localhost:9292',
                'localhost:9293',
                'localhost:9294',
                'localhost:9295',
                'localhost:9296',
                'localhost:9297',
                'localhost:9298',
                'localhost:9299'
              ]
                labels:
                  component: flink

  service:
    pipelines:
      flink:
        receivers: [prometheus_flink]
      otlp:
        receivers: [otlp]   # pipeline for OTLP metrics

combined:
  receivers:
    otlp:
      type: otlp

traces:
  service:
    pipelines:
      otlp:
        receivers: [otlp]
EOF

systemctl restart google-cloud-ops-agent
