require 'jruby/kafka'

class KafkaConsumer
  attr_reader :messages, :counter

  def initialize(zookeeper, topic, key_deserializer = :string, value_deserializer = :string, schema_repo_url=nil)
    @messages = []
    @counter = 0 #index of the first unread message
    @config = Java::JavaUtil::Properties.new
    @config['zookeeper.connect'] = zookeeper
    @config['auto.offset.reset'] = 'largest'
    @config['group.id'] = 'ruby_kafka_consumer'
    set_deserializers(key_deserializer, value_deserializer, schema_repo_url)

    @consumer = Java::KafkaConsumer::Consumer.createJavaConsumerConnector(Java::KafkaConsumer::ConsumerConfig.new @config)
    filter = Java::KafkaConsumer::Whitelist.new topic
    stream = @consumer.create_message_streams_by_filter(filter, 1, @key_deserializer, @value_deserializer).get 0
    iterator = stream.iterator
    @thread = get_reader_thread(iterator)
  end

  def get_message(timeout=60)
    start = Time.now
    message = @messages[@counter]
    while message.nil? && Time.now < (start + timeout)
      sleep 0.5
      message = @messages[@counter]
    end
    @counter += 1 if message
    return message
  end

  def clear_all_messages
    @counter = @messages.length
  end

  def get_reader_thread(iterator)
    Thread.new do
      while iterator.has_next?
        @messages << iterator.next.message
      end
    end
  end
  private :get_reader_thread

  def set_deserializers(key_deserializer, value_deserializer, schema_repo_url)
    if key_deserializer.to_s == "avro" || value_deserializer.to_s == "avro"
      raise ArgumentError, "schema_repo_url required with avro serializer" unless schema_repo_url
      @config['schema.registry.url'] = schema_repo_url
      if $DEBUG
        l = Java::OrgApacheLog4j::Logger.get_logger "io.confluent"
        l.set_level(Java::OrgApacheLog4j::Level::DEBUG)
        l.add_appender Java::OrgApacheLog4j::ConsoleAppender.new(Java::OrgApacheLog4j::SimpleLayout.new, Java::OrgApacheLog4j::ConsoleAppender::SYSTEM_OUT)
      end
    end
    vconfig = Java::KafkaUtils::VerifiableProperties.new @config

    case key_deserializer
    when :string, "string"
      @key_deserializer = Java::KafkaSerializer::StringDecoder.new nil
    when :byte_array, "byte_array"
      @key_deserializer = Java::KafkaSerializer::DefaultDecoder.new
    when :avro, "avro"
      require 'jruby/avro_serializer'
      @key_deserializer = Java::IoConfluentKafkaSerializers::KafkaAvroDecoder.new vconfig
    else
      raise ArgumentError, "key_deserializer must be :string, :byte_array, or :avro"
    end

    case value_deserializer
    when :string, "string"
      @value_deserializer = Java::KafkaSerializer::StringDecoder.new nil
    when :byte_array, "byte_array"
      @value_deserializer = Java::KafkaSerializer::DefaultDecoder.new
    when :avro, "avro"
      require 'jruby/avro_serializer'
      @value_deserializer = Java::IoConfluentKafkaSerializers::KafkaAvroDecoder.new vconfig
    else
      raise ArgumentError, "value_deserializer must be :string, :byte_array, or :avro"
    end
  end
  private :set_deserializers

  def close
    @thread.kill
    @consumer.shutdown
  end

end
