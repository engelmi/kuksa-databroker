/********************************************************************************
* Copyright (c) 2022 Contributors to the Eclipse Foundation
*
* See the NOTICE file(s) distributed with this work for additional
* information regarding copyright ownership.
*
* This program and the accompanying materials are made available under the
* terms of the Apache License 2.0 which is available at
* http://www.apache.org/licenses/LICENSE-2.0
*
* SPDX-License-Identifier: Apache-2.0
********************************************************************************/

use std::collections::HashMap;

use databroker_proto::sdv::databroker::v1 as proto;

use tokio::select;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Response, Status};
use tracing::debug;

use crate::{
    broker::{self, RegistrationError},
    permissions::Permissions,
};

#[tonic::async_trait]
impl proto::collector_server::Collector for broker::DataBroker {
    async fn update_datapoints(
        &self,
        request: tonic::Request<proto::UpdateDatapointsRequest>,
    ) -> Result<tonic::Response<proto::UpdateDatapointsReply>, tonic::Status> {
        debug!(?request);
        let permissions = match request.extensions().get::<Permissions>() {
            Some(permissions) => {
                debug!(?permissions);
                permissions.clone()
            }
            None => return Err(tonic::Status::unauthenticated("Unauthenticated")),
        };
        let broker = self.authorized_access(&permissions);

        // Collect errors encountered
        let mut errors = HashMap::new();

        let message = request.into_inner();
        let ids: Vec<(i32, broker::EntryUpdate)> = message
            .datapoints
            .iter()
            .map(|(id, datapoint)| {
                (
                    *id,
                    broker::EntryUpdate {
                        path: None,
                        datapoint: Some(broker::Datapoint::from(datapoint)),
                        actuator_target: None,
                        entry_type: None,
                        data_type: None,
                        description: None,
                        allowed: None,
                        max: None,
                        min: None,
                        unit: None,
                    },
                )
            })
            .collect();

        match broker.update_entries(ids).await {
            Ok(()) => {}
            Err(err) => {
                debug!("Failed to set datapoint: {:?}", err);
                errors = err
                    .iter()
                    .map(|(id, error)| (*id, proto::DatapointError::from(error) as i32))
                    .collect();
            }
        }

        Ok(Response::new(proto::UpdateDatapointsReply { errors }))
    }

    type StreamDatapointsStream = ReceiverStream<Result<proto::StreamDatapointsReply, Status>>;

    async fn stream_datapoints(
        &self,
        request: tonic::Request<tonic::Streaming<proto::StreamDatapointsRequest>>,
    ) -> Result<tonic::Response<Self::StreamDatapointsStream>, tonic::Status> {
        debug!(?request);
        let permissions = match request.extensions().get::<Permissions>() {
            Some(permissions) => {
                debug!(?permissions);
                permissions.clone()
            }
            None => return Err(tonic::Status::unauthenticated("Unauthenticated")),
        };

        let mut stream = request.into_inner();

        let mut shutdown_trigger = self.get_shutdown_trigger();

        // Copy (to move into task below)
        let broker = self.clone();

        // Create error stream (to be returned)
        let (error_sender, error_receiver) = mpsc::channel(10);

        // Listening on stream
        tokio::spawn(async move {
            let permissions = permissions;
            let broker = broker.authorized_access(&permissions);
            loop {
                select! {
                    message = stream.message() => {
                        match message {
                            Ok(request) => {
                                match request {
                                    Some(req) => {
                                        let ids: Vec<(i32, broker::EntryUpdate)> = req.datapoints
                                            .iter()
                                            .map(|(id, datapoint)|
                                                (
                                                    *id,
                                                    broker::EntryUpdate {
                                                        path: None,
                                                        datapoint: Some(broker::Datapoint::from(datapoint)),
                                                        actuator_target: None,
                                                        entry_type: None,
                                                        data_type: None,
                                                        description: None,
                                                        allowed: None,
                                                        max: None,
                                                        min: None,
                                                        unit: None,
                                                    }
                                                )
                                            )
                                            .collect();
                                        // TODO: Check if sender is allowed to provide datapoint with this id
                                        match broker
                                            .update_entries(ids)
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(err) => {
                                                if let Err(err) = error_sender.send(
                                                    Ok(proto::StreamDatapointsReply {
                                                        errors: err.iter().map(|(id, error)| {
                                                            (*id, proto::DatapointError::from(error) as i32)
                                                        }).collect(),
                                                    })
                                                ).await {
                                                    debug!("Failed to send errors: {}", err);
                                                }
                                            }
                                        }
                                    },
                                    None => {
                                        debug!("provider: no more messages");
                                        break;
                                    }
                                }
                            },
                            Err(err) => {
                                debug!("provider: connection broken: {:?}", err);
                                break;
                            },
                        }
                    },
                    _ = shutdown_trigger.recv() => {
                        debug!("provider: shutdown received");
                        break;
                    }
                }
            }
        });

        // Return the error stream
        Ok(Response::new(ReceiverStream::new(error_receiver)))
    }

    async fn register_datapoints(
        &self,
        request: tonic::Request<proto::RegisterDatapointsRequest>,
    ) -> Result<tonic::Response<proto::RegisterDatapointsReply>, Status> {
        debug!(?request);
        let permissions = match request.extensions().get::<Permissions>() {
            Some(permissions) => {
                debug!(?permissions);
                permissions.clone()
            }
            None => return Err(tonic::Status::unauthenticated("Unauthenticated")),
        };
        let broker = self.authorized_access(&permissions);

        let mut results = HashMap::new();
        let mut error = None;

        for metadata in request.into_inner().list {
            match (
                proto::DataType::try_from(metadata.data_type),
                proto::ChangeType::try_from(metadata.change_type),
            ) {
                (Ok(value_type), Ok(change_type)) => {
                    match broker
                        .add_entry(
                            metadata.name.clone(),
                            broker::DataType::from(&value_type),
                            broker::ChangeType::from(&change_type),
                            broker::types::EntryType::Sensor,
                            metadata.description,
                            None, // min
                            None, // max
                            None,
                            None,
                        )
                        .await
                    {
                        Ok(id) => results.insert(metadata.name, id),
                        Err(RegistrationError::PermissionDenied) => {
                            // Registration error
                            error = Some(Status::new(
                                Code::PermissionDenied,
                                format!("Failed to register {}", metadata.name),
                            ));
                            break;
                        }
                        Err(RegistrationError::PermissionExpired) => {
                            // Registration error
                            error = Some(Status::new(
                                Code::Unauthenticated,
                                format!("Failed to register {}", metadata.name),
                            ));
                            break;
                        }
                        Err(RegistrationError::ValidationError) => {
                            // Registration error
                            error = Some(Status::new(
                                Code::InvalidArgument,
                                format!("Failed to register {}", metadata.name),
                            ));
                            break;
                        }
                    };
                }
                (Err(_), _) => {
                    // Invalid data type
                    error = Some(Status::new(
                        Code::InvalidArgument,
                        format!("Unsupported data type provided for {}", metadata.name),
                    ));
                    break;
                }
                (_, Err(_)) => {
                    // Invalid change type
                    error = Some(Status::new(
                        Code::InvalidArgument,
                        format!("Unsupported change type provided for {}", metadata.name),
                    ));
                    break;
                }
            }
        }

        match error {
            Some(error) => Err(error),
            None => Ok(Response::new(proto::RegisterDatapointsReply { results })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{broker::DataBroker, permissions};
    use proto::collector_server::Collector;

    #[tokio::test]
    async fn test_publish_value_min_max_not_fulfilled() {
        let broker = DataBroker::default();
        let authorized_access = broker.authorized_access(&permissions::ALLOW_ALL);

        let entry_id_1 = authorized_access
            .add_entry(
                "test.datapoint1".to_owned(),
                broker::DataType::Uint8,
                broker::ChangeType::OnChange,
                broker::EntryType::Sensor,
                "Test datapoint 1".to_owned(),
                Some(broker::types::DataValue::Uint32(3)), // min
                Some(broker::types::DataValue::Uint32(26)), // max
                None,
                None,
            )
            .await
            .unwrap();

        let entry_id_2 = authorized_access
            .add_entry(
                "test.datapoint1.Speed".to_owned(),
                broker::DataType::Float,
                broker::ChangeType::OnChange,
                broker::EntryType::Sensor,
                "Test datapoint 1".to_owned(),
                Some(broker::types::DataValue::Float(1.0)), // min
                Some(broker::types::DataValue::Float(100.0)), // max
                None,
                None,
            )
            .await
            .unwrap();

        let datapoint: proto::Datapoint = proto::Datapoint {
            timestamp: None,
            value: Some(proto::datapoint::Value::Int32Value(50)),
        };

        let mut datapoints = HashMap::new();
        datapoints.insert(entry_id_1, datapoint.clone());
        datapoints.insert(entry_id_2, datapoint);

        let request = proto::UpdateDatapointsRequest { datapoints };

        // Manually insert permissions
        let mut publish_value_request = tonic::Request::new(request);
        publish_value_request
            .extensions_mut()
            .insert(permissions::ALLOW_ALL.clone());

        match broker.update_datapoints(publish_value_request).await {
            Ok(response) => {
                let response = response.into_inner();
                assert_eq!(response.errors.len(), 2);

                let error_entry_1 = response.errors.get(&entry_id_1);
                assert_eq!(
                    error_entry_1.unwrap().clone(),
                    proto::DatapointError::OutOfBounds as i32
                );

                let error_entry_2 = response.errors.get(&entry_id_2);
                assert_eq!(
                    error_entry_2.unwrap().clone(),
                    proto::DatapointError::InvalidType as i32
                );
            }
            Err(_) => {
                panic!("Should not happen!");
            }
        }
    }
}
