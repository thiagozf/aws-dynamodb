const { mergeDeepRight, pick } = require('ramda')
const AWS = require('aws-sdk')
const { Component } = require('@serverless/core')
const {
  log,
  createTable,
  deleteTable,
  describeTable,
  updateTable,
  updateReplicas,
  deleteReplicas
} = require('./utils')

const outputsList = ['name', 'arn', 'region', 'streamArn']

const defaults = {
  attributeDefinitions: [
    {
      AttributeName: 'id',
      AttributeType: 'S'
    }
  ],
  keySchema: [
    {
      AttributeName: 'id',
      KeyType: 'HASH'
    }
  ],
  globalSecondaryIndexes: [],
  name: null,
  region: 'us-east-1',
  replicas: [],
  stream: {
    StreamEnabled: false
  }
}

class AwsDynamoDb extends Component {
  async deploy(inputs = {}) {
    // this error message assumes that the user is running via the CLI though...
    if (Object.keys(this.credentials.aws).length === 0) {
      const msg = `Credentials not found. Make sure you have a .env file in the cwd. - Docs: https://git.io/JvArp`
      throw new Error(msg)
    }

    const config = mergeDeepRight(defaults, inputs)

    // If first deploy and no name is found, set default name..
    if (!config.name && !this.state.name) {
      config.name =
        'dynamodb-table-' +
        Math.random()
          .toString(36)
          .substring(6)
      this.state.name = config.name
    }
    // If first deploy, and a name is set...
    else if (config.name && !this.state.name) {
      this.state.name = config.name
    }
    // If subequent deploy, and name is different from a previously used name, throw error.
    else if (config.name && this.state.name && config.name !== this.state.name) {
      throw new Error(
        `You cannot change the name of your DynamoDB table once it has been deployed (or this will deploy a new table).  Please remove this Component Instance first by running "serverless remove", then redeploy it with "serverless deploy".`
      )
    }

    console.log(`Starting deployment of table ${config.name} in the ${config.region} region.`)

    const dynamodb = new AWS.DynamoDB({
      region: config.region,
      credentials: this.credentials.aws
    })

    console.log(`Checking if table ${config.name} already exists in the ${config.region} region.`)

    const prevTable = await describeTable({ dynamodb, name: config.name })

    if (!prevTable) {
      log(`Table ${config.name} does not exist. Creating...`)

      const { tableArn, streamArn } = await createTable({ dynamodb, ...config })
      config.arn = tableArn
      config.streamArn = streamArn
    } else {
      console.log(`Table ${config.name} already exists. Comparing config changes...`)

      // Check region
      if (config.region !== this.state.region) {
        throw new Error(
          'You cannot change the region of a DynamoDB Table.  Please remove it and redeploy in your desired region.'
        )
      }

      config.arn = prevTable.arn

      const prevGlobalSecondaryIndexes = prevTable.globalSecondaryIndexes
      const prevStream = prevTable.stream
      const { streamArn } = await updateTable.call(this, {
        dynamodb,
        prevGlobalSecondaryIndexes,
        prevStream,
        ...config
      })
      config.streamArn = streamArn
    }

    log(`Table ${config.name} was successfully deployed to the ${config.region} region.`)

    this.state.arn = config.arn
    this.state.streamArn = config.streamArn
    this.state.name = config.name
    this.state.region = config.region
    this.state.deletionPolicy = config.deletionPolicy

    const prevReplicas = (prevTable && prevTable.replicas) || []
    await updateReplicas.call(this, { dynamodb, prevReplicas, ...config })

    const outputs = pick(outputsList, config)
    return outputs
  }

  /**
   * Remove
   */
  async remove(inputs = {}) {
    console.log('Removing')

    // If "delete: false", don't delete the table, and warn instead
    if (!this.state.deletionPolicy || this.state.deletionPolicy !== 'delete') {
      console.log(`Skipping table removal because "deletionPolicy" is not set to "delete".`)
      return {}
    }

    const { name, region } = this.state

    if (!name) {
      console.log(`Aborting removal. Table name not found in state.`)
      return
    }

    const dynamodb = new AWS.DynamoDB({
      region,
      credentials: this.credentials.aws
    })

    console.log(`Removing table ${name} from the ${region} region.`)

    const table = await describeTable({ dynamodb, name })
    await deleteReplicas({ dynamodb, name, replicas: table.replicas })
    await deleteTable({ dynamodb, name })

    const outputs = pick(outputsList, this.state)

    console.log(`Table ${name} was successfully removed from the ${region} region.`)

    this.state = {}
    return outputs
  }
}

module.exports = AwsDynamoDb
