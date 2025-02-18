let lib = ./lib.dhall

let local = \(port : Natural) -> { host = "127.0.0.1", port }

in

{ roster =
  [ { admin_address = local 8780
    , client_address = local 8880
    , peer_address = local 8980
    }
  , { admin_address = local 8781
    , client_address = local 8881
    , peer_address = local 8981
    }
  , { admin_address = local 8782
    , client_address = local 8882
    , peer_address = local 8982
    }
  ]

, schedule =
  [ { network_id = "local"
    , competitor_id = "etcd"
    , cluster_size = 3
    , request_ops_per_s = 1000
    , request_total = 10000
    , client_count = 10
    , message_latency_ms = 0
    }
  , { network_id = "local"
    , competitor_id = "ferry"
    , cluster_size = 3
    , request_ops_per_s = 1000
    , request_total = 10000
    , client_count = 10
    , message_latency_ms = 0
    }
  ]
}
: lib.NetworkConfig
