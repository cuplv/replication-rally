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
  , { admin_address = local 8783
    , client_address = local 8883
    , peer_address = local 8983
    }
  , { admin_address = local 8784
    , client_address = local 8884
    , peer_address = local 8984
    }
  ]

, schedule =
  [ { network_id = "local"
    , competitor_id = "ferry"
    , cluster_size = 5
    , message_latency_ms = 0
    , tuning_parameters =
      { election_timeout_ms_low = 150
      , election_timeout_ms_high = 300
      , heartbeat_timeout_ms = 10
      , retransmission_timeout_ms = 200
      }
    }
  , { network_id = "local"
    , competitor_id = "etcd"
    , cluster_size = 5
    , message_latency_ms = 0
    , tuning_parameters =
      { election_timeout_ms_low = 150
      , election_timeout_ms_high = 300
      , heartbeat_timeout_ms = 10
      , retransmission_timeout_ms = 200
      }
    }
  ]
}
