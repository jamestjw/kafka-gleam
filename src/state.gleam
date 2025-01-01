import gleam/dict.{type Dict}
import gleam/list
import gleam/result
import parser/metadata.{type Record, type RecordBatch}

pub type Partition {
  Partition(
    id: Int,
    topic_uuid: Int,
    replica_ids: List(Int),
    in_sync_replica_ids: List(Int),
    removing_replica_ids: List(Int),
    adding_replica_ids: List(Int),
    leader_replica_id: Int,
    leader_epoch: Int,
    partition_epoch: Int,
    directory_uuids: List(Int),
  )
}

pub type State {
  State(
    topic_to_id: Dict(String, Int),
    topic_uuid_to_partition: Dict(Int, List(Partition)),
  )
}

pub fn new() {
  State(topic_to_id: dict.new(), topic_uuid_to_partition: dict.new())
}

pub fn load_record_batches(state, record_batches: List(RecordBatch)) {
  let load_record = fn(state, record: Record) {
    let State(topic_to_id, topic_uuid_to_partition) = state
    case record.value {
      metadata.TopicValue(topic_name, topic_uuid) -> {
        State(
          ..state,
          topic_to_id: dict.insert(topic_to_id, topic_name, topic_uuid),
        )
      }
      metadata.PartitionValue(
        partition_id:,
        topic_uuid:,
        replica_ids:,
        in_sync_replica_ids:,
        removing_replica_ids:,
        adding_replica_ids:,
        leader_replica_id:,
        leader_epoch:,
        partition_epoch:,
        directory_uuids:,
      ) -> {
        let partition =
          Partition(
            id: partition_id,
            topic_uuid:,
            replica_ids:,
            in_sync_replica_ids:,
            removing_replica_ids:,
            adding_replica_ids:,
            leader_replica_id:,
            leader_epoch:,
            partition_epoch:,
            directory_uuids:,
          )
        let topic_uuid_to_partition =
          dict.get(topic_uuid_to_partition, topic_uuid)
          |> result.unwrap([])
          |> list.prepend(partition)
          |> dict.insert(topic_uuid_to_partition, topic_uuid, _)

        State(..state, topic_uuid_to_partition:)
      }

      _ -> state
    }
  }
  let load_record_batch = fn(state, record_batch: RecordBatch) {
    list.fold(record_batch.records, state, load_record)
  }
  list.fold(record_batches, state, load_record_batch)
}
