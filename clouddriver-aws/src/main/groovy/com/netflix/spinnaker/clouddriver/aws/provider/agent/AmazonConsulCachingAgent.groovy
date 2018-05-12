package com.netflix.spinnaker.clouddriver.aws.provider.agent

import com.amazonaws.services.ec2.model.DescribeInstancesRequest
import com.amazonaws.services.ec2.model.Instance
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.netflix.spinnaker.cats.agent.AccountAware
import com.netflix.spinnaker.cats.agent.AgentDataType
import com.netflix.spinnaker.cats.agent.CacheResult
import com.netflix.spinnaker.cats.agent.CachingAgent
import com.netflix.spinnaker.cats.agent.DefaultCacheResult
import com.netflix.spinnaker.cats.cache.CacheData
import com.netflix.spinnaker.cats.provider.ProviderCache
import com.netflix.spinnaker.clouddriver.aws.provider.AwsProvider
import com.netflix.spinnaker.clouddriver.aws.security.NetflixAmazonCredentials
import com.netflix.spinnaker.clouddriver.aws.security.AmazonClientProvider
import com.netflix.spinnaker.clouddriver.consul.config.ConsulProperties
import com.netflix.spinnaker.clouddriver.consul.model.ConsulNode
import com.netflix.spinnaker.clouddriver.consul.provider.ConsulProviderUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import retrofit.RetrofitError

import static com.netflix.spinnaker.cats.agent.AgentDataType.Authority.AUTHORITATIVE
import static com.netflix.spinnaker.clouddriver.core.provider.agent.Namespace.INSTANCES

// TODO: might need to implement HealthProvidingCachingAgent as well
class AmazonConsulCachingAgent implements CachingAgent, AccountAware {
  final Logger log = LoggerFactory.getLogger(getClass())

  final Set<AgentDataType> types = Collections.unmodifiableSet([
    AUTHORITATIVE.forType(INSTANCES.ns)
  ] as Set)

  final AmazonClientProvider amazonClientProvider
  final NetflixAmazonCredentials account
  final String region
  final ObjectMapper objectMapper
  final ApplicationContext ctx

  AmazonConsulCachingAgent(AmazonClientProvider amazonClientProvider,
                           NetflixAmazonCredentials account, String region,
                           ObjectMapper objectMapper,
                           ApplicationContext ctx) {
    this.amazonClientProvider = amazonClientProvider
    this.account = account
    this.region = region
    this.objectMapper = objectMapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    this.ctx = ctx
  }

  @Override
  Collection<AgentDataType> getProvidedDataTypes() {
    types
  }

  @Override
  String getAccountName() {
    account.name
  }

  @Override
  String getProviderName() {
    AwsProvider.PROVIDER_NAME
  }

  @Override
  String getAgentType() {
    "${account.name}/${region}/${AmazonConsulCachingAgent.simpleName}"
  }

  CacheResult loadData(ProviderCache providerCache) {
    log.info("Describing items in ${agentType}")

    def amazonEC2 = amazonClientProvider.getAmazonEC2(account, region)

    Long start = null
    def request = new DescribeInstancesRequest().withMaxResults(500)
    List<Instance> awsInstances = []
    while (true) {
      def resp = amazonEC2.describeInstances(request)
      if (account.consulConfig?.enabled) {
        resp.reservations.each { server ->
          ConsulNode consulNode = null
          try{
            log.info("Getting health from ${server.instances.instanceId.toString()}")
            consulNode = ConsulProviderUtils.getHealths(account.consulConfig, server.instances.instanceId.toString())
            log.info("getHealths returned this consulNode: ${consulNode}")
          } catch (RetrofitError e) {
            log.warn(e.message)
          }
        }
      }
      // awsInstances.addAll(resp.reservations.collectMany { it.instances })
      if (resp.nextToken) {
        request.withNextToken(resp.nextToken)
      } else {
        break
      }
    }

    // TODO: MutableCacheData is referencing a class in AbstractAmazonLoadBalancerCachingAgent, probably want new one.
    Closure<Map<String, CacheData>> cache = {
      [:].withDefault { String id -> new MutableCacheData(id) }
    }

    Map<String, CacheData> instances = cache()

    new DefaultCacheResult(
      (INSTANCES.ns): instances.values(),
    )
  }
}
