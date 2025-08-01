// DO NOT EDIT : This file is generated by presto_protocol-to-thrift-json.py
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

// This file is generated DO NOT EDIT @generated

#include "presto_cpp/main/thrift/gen-cpp2/presto_thrift_types.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace facebook::presto::thrift {

void toThrift(
    const facebook::presto::protocol::Duration& duration,
    double& thrift);
void toThrift(
    const facebook::presto::protocol::DataSize& dataSize,
    double& thrift);
void fromThrift(
    const double& thrift,
    facebook::presto::protocol::Duration& duration);
void fromThrift(
    const double& thrift,
    facebook::presto::protocol::DataSize& dataSize);

void toThrift(
    const facebook::presto::protocol::TaskState& proto,
    TaskState& thrift);
void fromThrift(
    const TaskState& thrift,
    facebook::presto::protocol::TaskState& proto);

void toThrift(
    const facebook::presto::protocol::ErrorType& proto,
    ErrorType& thrift);
void fromThrift(
    const ErrorType& thrift,
    facebook::presto::protocol::ErrorType& proto);

void toThrift(
    const facebook::presto::protocol::ErrorCause& proto,
    ErrorCause& thrift);
void fromThrift(
    const ErrorCause& thrift,
    facebook::presto::protocol::ErrorCause& proto);

void toThrift(
    const facebook::presto::protocol::BufferState& proto,
    BufferState& thrift);
void fromThrift(
    const BufferState& thrift,
    facebook::presto::protocol::BufferState& proto);

void toThrift(
    const facebook::presto::protocol::BlockedReason& proto,
    BlockedReason& thrift);
void fromThrift(
    const BlockedReason& thrift,
    facebook::presto::protocol::BlockedReason& proto);

void toThrift(
    const facebook::presto::protocol::RuntimeUnit& proto,
    RuntimeUnit& thrift);
void fromThrift(
    const RuntimeUnit& thrift,
    facebook::presto::protocol::RuntimeUnit& proto);

void toThrift(
    const facebook::presto::protocol::JoinType& proto,
    JoinType& thrift);
void fromThrift(
    const JoinType& thrift,
    facebook::presto::protocol::JoinType& proto);

void toThrift(const facebook::presto::protocol::Type& proto, Type& thrift);
void fromThrift(const Type& thrift, facebook::presto::protocol::Type& proto);

void toThrift(
    const facebook::presto::protocol::Determinism& proto,
    Determinism& thrift);
void fromThrift(
    const Determinism& thrift,
    facebook::presto::protocol::Determinism& proto);

void toThrift(
    const facebook::presto::protocol::NullCallClause& proto,
    NullCallClause& thrift);
void fromThrift(
    const NullCallClause& thrift,
    facebook::presto::protocol::NullCallClause& proto);

void toThrift(
    const facebook::presto::protocol::FunctionKind& proto,
    FunctionKind& thrift);
void fromThrift(
    const FunctionKind& thrift,
    facebook::presto::protocol::FunctionKind& proto);

void toThrift(
    const facebook::presto::protocol::BufferType& proto,
    BufferType& thrift);
void fromThrift(
    const BufferType& thrift,
    facebook::presto::protocol::BufferType& proto);

void toThrift(
    const facebook::presto::protocol::TableWriteInfo& tableWriteInfo,
    TableWriteInfoWrapper& thriftTableWriteInfoWrapper);
void toThrift(
    const facebook::presto::protocol::TableWriteInfo& tableWriteInfo,
    std::string& thriftTableWriteInfo);
void fromThrift(
    const TableWriteInfoWrapper& thriftTableWriteInfoWrapper,
    facebook::presto::protocol::TableWriteInfo& tableWriteInfo);
void fromThrift(
    const std::string& thriftTableWriteInfo,
    facebook::presto::protocol::TableWriteInfo& tableWriteInfo);
void toThrift(
    const std::shared_ptr<facebook::presto::protocol::ConnectorSplit>& proto,
    ConnectorSplitWrapper& thrift);
void fromThrift(
    const ConnectorSplitWrapper& thrift,
    std::shared_ptr<facebook::presto::protocol::ConnectorSplit>& proto);
void toThrift(
    const std::shared_ptr<
        facebook::presto::protocol::ConnectorTransactionHandle>& proto,
    ConnectorTransactionHandleWrapper& thrift);
void fromThrift(
    const ConnectorTransactionHandleWrapper& thrift,
    std::shared_ptr<facebook::presto::protocol::ConnectorTransactionHandle>&
        proto);
void toThrift(
    const facebook::presto::protocol::Lifespan& proto,
    Lifespan& thrift);
void fromThrift(
    const Lifespan& thrift,
    facebook::presto::protocol::Lifespan& proto);

void toThrift(
    const facebook::presto::protocol::ErrorLocation& proto,
    ErrorLocation& thrift);
void fromThrift(
    const ErrorLocation& thrift,
    facebook::presto::protocol::ErrorLocation& proto);

void toThrift(
    const facebook::presto::protocol::HostAddress& proto,
    HostAddress& thrift);
void fromThrift(
    const HostAddress& thrift,
    facebook::presto::protocol::HostAddress& proto);

void toThrift(
    const facebook::presto::protocol::OutputBufferId& proto,
    OutputBufferId& thrift);
void fromThrift(
    const OutputBufferId& thrift,
    facebook::presto::protocol::OutputBufferId& proto);

void toThrift(
    const facebook::presto::protocol::PageBufferInfo& proto,
    PageBufferInfo& thrift);
void fromThrift(
    const PageBufferInfo& thrift,
    facebook::presto::protocol::PageBufferInfo& proto);

void toThrift(
    const facebook::presto::protocol::PlanNodeId& proto,
    PlanNodeId& thrift);
void fromThrift(
    const PlanNodeId& thrift,
    facebook::presto::protocol::PlanNodeId& proto);

void toThrift(
    const facebook::presto::protocol::DistributionSnapshot& proto,
    DistributionSnapshot& thrift);
void fromThrift(
    const DistributionSnapshot& thrift,
    facebook::presto::protocol::DistributionSnapshot& proto);

void toThrift(
    const facebook::presto::protocol::RuntimeStats& proto,
    RuntimeStats& thrift);
void fromThrift(
    const RuntimeStats& thrift,
    facebook::presto::protocol::RuntimeStats& proto);

void toThrift(
    const facebook::presto::protocol::DynamicFilterStats& proto,
    DynamicFilterStats& thrift);
void fromThrift(
    const DynamicFilterStats& thrift,
    facebook::presto::protocol::DynamicFilterStats& proto);

void toThrift(
    const facebook::presto::protocol::DriverStats& proto,
    DriverStats& thrift);
void fromThrift(
    const DriverStats& thrift,
    facebook::presto::protocol::DriverStats& proto);

void toThrift(
    const facebook::presto::protocol::TransactionId& proto,
    TransactionId& thrift);
void fromThrift(
    const TransactionId& thrift,
    facebook::presto::protocol::TransactionId& proto);

void toThrift(
    const facebook::presto::protocol::TimeZoneKey& proto,
    TimeZoneKey& thrift);
void fromThrift(
    const TimeZoneKey& thrift,
    facebook::presto::protocol::TimeZoneKey& proto);

void toThrift(
    const facebook::presto::protocol::ResourceEstimates& proto,
    ResourceEstimates& thrift);
void fromThrift(
    const ResourceEstimates& thrift,
    facebook::presto::protocol::ResourceEstimates& proto);

void toThrift(
    const facebook::presto::protocol::ConnectorId& proto,
    ConnectorId& thrift);
void fromThrift(
    const ConnectorId& thrift,
    facebook::presto::protocol::ConnectorId& proto);

void toThrift(
    const facebook::presto::protocol::SqlFunctionId& proto,
    SqlFunctionId& thrift);
void fromThrift(
    const SqlFunctionId& thrift,
    facebook::presto::protocol::SqlFunctionId& proto);

void toThrift(
    const facebook::presto::protocol::TypeSignature& proto,
    TypeSignature& thrift);
void fromThrift(
    const TypeSignature& thrift,
    facebook::presto::protocol::TypeSignature& proto);

void toThrift(
    const facebook::presto::protocol::Language& proto,
    Language& thrift);
void fromThrift(
    const Language& thrift,
    facebook::presto::protocol::Language& proto);

void toThrift(
    const facebook::presto::protocol::QualifiedObjectName& proto,
    QualifiedObjectName& thrift);
void fromThrift(
    const QualifiedObjectName& thrift,
    facebook::presto::protocol::QualifiedObjectName& proto);

void toThrift(
    const facebook::presto::protocol::TypeVariableConstraint& proto,
    TypeVariableConstraint& thrift);
void fromThrift(
    const TypeVariableConstraint& thrift,
    facebook::presto::protocol::TypeVariableConstraint& proto);

void toThrift(
    const facebook::presto::protocol::LongVariableConstraint& proto,
    LongVariableConstraint& thrift);
void fromThrift(
    const LongVariableConstraint& thrift,
    facebook::presto::protocol::LongVariableConstraint& proto);

void toThrift(
    const facebook::presto::protocol::TaskSource& proto,
    TaskSource& thrift);
void fromThrift(
    const TaskSource& thrift,
    facebook::presto::protocol::TaskSource& proto);

void toThrift(
    const facebook::presto::protocol::SplitContext& proto,
    SplitContext& thrift);
void fromThrift(
    const SplitContext& thrift,
    facebook::presto::protocol::SplitContext& proto);

void toThrift(
    const facebook::presto::protocol::TaskStatus& proto,
    TaskStatus& thrift);
void fromThrift(
    const TaskStatus& thrift,
    facebook::presto::protocol::TaskStatus& proto);

void toThrift(
    const facebook::presto::protocol::ErrorCode& proto,
    ErrorCode& thrift);
void fromThrift(
    const ErrorCode& thrift,
    facebook::presto::protocol::ErrorCode& proto);

void toThrift(
    const facebook::presto::protocol::OutputBufferInfo& proto,
    OutputBufferInfo& thrift);
void fromThrift(
    const OutputBufferInfo& thrift,
    facebook::presto::protocol::OutputBufferInfo& proto);

void toThrift(
    const facebook::presto::protocol::BufferInfo& proto,
    BufferInfo& thrift);
void fromThrift(
    const BufferInfo& thrift,
    facebook::presto::protocol::BufferInfo& proto);

void toThrift(
    const facebook::presto::protocol::TaskStats& proto,
    TaskStats& thrift);
void fromThrift(
    const TaskStats& thrift,
    facebook::presto::protocol::TaskStats& proto);

void toThrift(
    const facebook::presto::protocol::PipelineStats& proto,
    PipelineStats& thrift);
void fromThrift(
    const PipelineStats& thrift,
    facebook::presto::protocol::PipelineStats& proto);

void toThrift(
    const facebook::presto::protocol::RuntimeMetric& proto,
    RuntimeMetric& thrift);
void fromThrift(
    const RuntimeMetric& thrift,
    facebook::presto::protocol::RuntimeMetric& proto);

void toThrift(
    const facebook::presto::protocol::SessionRepresentation& proto,
    SessionRepresentation& thrift);
void fromThrift(
    const SessionRepresentation& thrift,
    facebook::presto::protocol::SessionRepresentation& proto);

void toThrift(
    const facebook::presto::protocol::SelectedRole& proto,
    SelectedRole& thrift);
void fromThrift(
    const SelectedRole& thrift,
    facebook::presto::protocol::SelectedRole& proto);

void toThrift(
    const facebook::presto::protocol::Parameter& proto,
    Parameter& thrift);
void fromThrift(
    const Parameter& thrift,
    facebook::presto::protocol::Parameter& proto);

void toThrift(
    const facebook::presto::protocol::RoutineCharacteristics& proto,
    RoutineCharacteristics& thrift);
void fromThrift(
    const RoutineCharacteristics& thrift,
    facebook::presto::protocol::RoutineCharacteristics& proto);

void toThrift(
    const facebook::presto::protocol::Signature& proto,
    Signature& thrift);
void fromThrift(
    const Signature& thrift,
    facebook::presto::protocol::Signature& proto);

void toThrift(const facebook::presto::protocol::Split& proto, Split& thrift);
void fromThrift(const Split& thrift, facebook::presto::protocol::Split& proto);

void toThrift(
    const facebook::presto::protocol::OutputBuffers& proto,
    OutputBuffers& thrift);
void fromThrift(
    const OutputBuffers& thrift,
    facebook::presto::protocol::OutputBuffers& proto);

void toThrift(
    const facebook::presto::protocol::TaskUpdateRequest& proto,
    TaskUpdateRequest& thrift);
void fromThrift(
    const TaskUpdateRequest& thrift,
    facebook::presto::protocol::TaskUpdateRequest& proto);

void toThrift(
    const facebook::presto::protocol::ExecutionFailureInfo& proto,
    ExecutionFailureInfo& thrift);
void fromThrift(
    const ExecutionFailureInfo& thrift,
    facebook::presto::protocol::ExecutionFailureInfo& proto);

void toThrift(const facebook::presto::protocol::TaskId& proto, TaskId& thrift);
void fromThrift(
    const TaskId& thrift,
    facebook::presto::protocol::TaskId& proto);

void toThrift(
    const facebook::presto::protocol::OperatorInfo& proto,
    OperatorInfoUnion& thrift);
void fromThrift(
    const OperatorInfoUnion& thrift,
    facebook::presto::protocol::OperatorInfo& proto);
void toThrift(
    const facebook::presto::protocol::SqlInvokedFunction& proto,
    SqlInvokedFunction& thrift);
void fromThrift(
    const SqlInvokedFunction& thrift,
    facebook::presto::protocol::SqlInvokedFunction& proto);

void toThrift(
    const facebook::presto::protocol::ScheduledSplit& proto,
    ScheduledSplit& thrift);
void fromThrift(
    const ScheduledSplit& thrift,
    facebook::presto::protocol::ScheduledSplit& proto);

void toThrift(
    const facebook::presto::protocol::TaskInfo& proto,
    TaskInfo& thrift);
void fromThrift(
    const TaskInfo& thrift,
    facebook::presto::protocol::TaskInfo& proto);

void toThrift(
    const facebook::presto::protocol::OperatorStats& proto,
    OperatorStats& thrift);
void fromThrift(
    const OperatorStats& thrift,
    facebook::presto::protocol::OperatorStats& proto);

} // namespace facebook::presto::thrift
