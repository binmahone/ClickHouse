//
// Created by hongbin on 23-4-17.
//

#include <iostream>
#include <memory>
#include "Shuffle/ShuffleReader.h"
#include <IO/ReadBufferFromFile.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <Poco/Util/MapConfiguration.h>
#include <Builder/SerializedPlanBuilder.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashJoin.h>
#include <Parser/SparkRowToCHColumn.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/CustomMergeTreeSink.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TreeRewriter.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/SerializedPlanParser.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Shuffle/ShuffleReader.h>
#include <Shuffle/ShuffleSplitter.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/CustomMergeTreeSink.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <Common/logger_useful.h>
#include <Common/DebugUtils.h>
#include <Common/Logger.h>
#include <Common/MergeTreeTool.h>
#include <Common/PODArray_fwd.h>
#include <Common/Stopwatch.h>
#include <substrait/plan.pb.h>


using namespace local_engine;
using namespace DB;

DB::ContextMutablePtr global_context;

int main(){
    std::cout << "hello" <<std::endl;
    //local_engine::Logger::initConsoleLogger();
    SharedContextHolder shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();

    auto config = Poco::AutoPtr(new Poco::Util::MapConfiguration());
    global_context->setConfig(config);
    const std::string path = "/";
    global_context->setPath(path);
    SerializedPlanParser::global_context = global_context;
    local_engine::SerializedPlanParser::initFunctionEnv();

    auto & factory = local_engine::ReadBufferBuilderFactory::instance();
    registerReadBufferBuildes(factory);


    auto read_buffer = std::make_unique<DB::ReadBufferFromFile>("/tmp/blockmgr-2b394642-2d8a-4f94-a671-11f6fe5fcf73/0c/shuffle_0_0_0.data");
    //        read_buffer->seek(357841655, SEEK_SET);
    auto shuffle_reader = local_engine::ShuffleReader(std::move(read_buffer), true);
    DB::Block * block;
    int sum = 0;
    do
    {
        std::cout << "one block" << sum << std::endl;
        block = shuffle_reader.read();
        sum += block->rows();
    } while (block->columns() != 0);
    std::cout << "total rows:" << sum << std::endl;
}