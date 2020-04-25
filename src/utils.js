const { isEmpty, equals, difference } = require('ramda')

function log(msg) {
  return console.log(msg)
}

async function createTable({
  dynamodb,
  name,
  attributeDefinitions,
  keySchema,
  globalSecondaryIndexes,
  localSecondaryIndexes,
  stream
}) {
  const res = await dynamodb
    .createTable({
      TableName: name,
      AttributeDefinitions: attributeDefinitions,
      KeySchema: keySchema,
      GlobalSecondaryIndexes: globalSecondaryIndexes.length ? globalSecondaryIndexes : undefined,
      LocalSecondaryIndexes: localSecondaryIndexes,
      BillingMode: 'PAY_PER_REQUEST',
      StreamSpecification: stream
    })
    .promise()
  return {
    tableArn: res.TableDescription.TableArn,
    streamArn: res.TableDescription.LatestStreamArn || false
  }
}

async function describeTable({ dynamodb, name }) {
  let res

  try {
    const data = await dynamodb.describeTable({ TableName: name }).promise()
    res = {
      arn: data.Table.TableArn,
      name: data.Table.TableName,
      attributeDefinitions: data.Table.AttributeDefinitions,
      keySchema: data.Table.KeySchema,
      globalSecondaryIndexes: data.Table.GlobalSecondaryIndexes,
      stream: data.Table.StreamSpecification,
      streamArn: data.Table.LatestStreamArn,
      replicas: data.Table.Replicas
    }
  } catch (error) {
    if (error.code === 'ResourceNotFoundException') {
      res = null
    }
  } finally {
    return res
  }
}

function getGlobalIndexesUpdates({ prevGlobalSecondaryIndexes = [], globalSecondaryIndexes = [] }) {
  const indexUpdates = {}

  // find a globalSecondaryIndex that is not in any previous globalSecondaryIndex
  const toCreate = globalSecondaryIndexes.filter(
    (globalSecondardyIndex) =>
      prevGlobalSecondaryIndexes.findIndex(
        (element) => element.IndexName === globalSecondardyIndex.IndexName
      ) === -1
  )

  // If previous globalSecondaryIndex has an item that is not now present, then delete
  const toDelete = prevGlobalSecondaryIndexes
    .filter(
      (prevGlobalSecondaryIndex) =>
        globalSecondaryIndexes.findIndex(
          (element) => element.IndexName === prevGlobalSecondaryIndex.IndexName
        ) === -1
    )
    .map(({ IndexName }) => ({ IndexName }))

  // Only take the first item since only one delete and create can be done at a time
  if (toCreate.length) {
    indexUpdates.Create = toCreate[0]
    if (toCreate.length > 1) {
      console.log(
        `Only ${toCreate[0].IndexName} will be created since a limitation of DynamoDB is that only one Gloabl secondary index can be created during an upate.
          Run this operation after the index has been created on AWS to create the additional indexes`
      )
    }
  }

  if (toDelete.length) {
    indexUpdates.Delete = toDelete[0]
    if (toDelete.length > 1) {
      console.log(
        `Only ${toDelete[0].IndexName} will be deleted since a limitation of DynamoDB is that only one Gloabl secondary index can be deleted during an upate.
          Run this operation after the index has been deleted on AWS to delete the additional indexes`
      )
    }
  }

  return indexUpdates
}

function getStreamUpdates({ prevStream, stream }) {
  const disabledStream =
    prevStream && prevStream.StreamEnabled && (!stream || !stream.StreamEnabled)
  if (disabledStream) {
    return {
      StreamEnabled: false
    }
  }

  const enabledStream = (!prevStream || !prevStream.StreamEnabled) && stream && stream.StreamEnabled
  if (enabledStream) {
    return stream
  }

  return null
}

async function updateTable({
  dynamodb,
  prevGlobalSecondaryIndexes,
  globalSecondaryIndexes,
  name,
  attributeDefinitions,
  prevStream,
  stream
}) {
  const indexUpdates = getGlobalIndexesUpdates({
    prevGlobalSecondaryIndexes,
    globalSecondaryIndexes
  })

  const streamUpdates = getStreamUpdates({
    prevStream,
    stream
  })

  const res = await dynamodb
    .updateTable({
      TableName: name,
      AttributeDefinitions: attributeDefinitions,
      BillingMode: 'PAY_PER_REQUEST',
      GlobalSecondaryIndexUpdates: !isEmpty(indexUpdates) ? [indexUpdates] : undefined,
      ...(streamUpdates && {
        StreamSpecification: streamUpdates
      })
    })
    .promise()

  let streamArn = false
  if (stream.StreamEnabled) {
    streamArn = await getStreamArn({ dynamodb, name })
  }

  return {
    tableArn: res.TableDescription.TableArn,
    streamArn
  }
}

async function waitForActive({ dynamodb, name }) {
  console.log('Waiting for ACTIVE global table status...')
  await dynamodb.waitFor('tableExists', { TableName: name }).promise()
}

async function updateReplicas({ dynamodb, name, prevReplicas = [], replicas = [] }) {
  const prevRegions = prevReplicas.map((rg) => rg.RegionName).sort()
  const currentRegions = replicas.map((rg) => rg.RegionName).sort()
  if (equals(prevRegions, currentRegions)) {
    return
  }

  const addReplicas = difference(currentRegions, prevRegions).map((r) => ({
    Create: { RegionName: r }
  }))
  const deleteReplicas = difference(prevRegions, currentRegions).map((r) => ({
    Delete: { RegionName: r }
  }))

  await waitForActive({ dynamodb, name })
  await dynamodb
    .updateTable({
      TableName: name,
      ReplicaUpdates: [...addReplicas, ...deleteReplicas]
    })
    .promise()
}

async function deleteReplicas({ dynamodb, name, replicas = [] }) {
  if (isEmpty(replicas)) {
    return
  }

  await dynamodb
    .updateTable({
      TableName: name,
      ReplicaUpdates: replicas.map((r) => ({
        Delete: { RegionName: r.RegionName }
      }))
    })
    .promise()

  await waitForActive({ dynamodb, name })
}

async function deleteTable({ dynamodb, name }) {
  let res = false
  try {
    res = await dynamodb
      .deleteTable({
        TableName: name
      })
      .promise()
  } catch (error) {
    if (error.code !== 'ResourceNotFoundException') {
      throw error
    }
  }
  return !!res
}

async function getStreamArn({ dynamodb, name }) {
  const maxTries = 5
  let tries = 0

  do {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
    await sleep(10000)
    const { streamArn } = await describeTable({ dynamodb, name })
    if (streamArn) {
      return streamArn
    }
    tries++
  } while (tries < maxTries)

  throw Error(`There was a problem getting the arn for your DynamoDB stream. Please try again.`)
}

module.exports = {
  log,
  createTable,
  describeTable,
  updateTable,
  deleteTable,
  updateReplicas,
  deleteReplicas
}
