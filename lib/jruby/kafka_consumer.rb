gem 'jruby-kafka', "~> 0.8.2"
require 'jruby/kafka'

class KafkaConsumer
  attr_reader :queue

  def initialize(zookeeper, topic, key_deserializer, value_deserializer, opts={})
    @queue = Queue.new
    @config = Java::JavaUtil::Properties.new
    @config['zookeeper.connect'] = zookeeper
    @config['auto.offset.reset'] = 'largest'
    @config['group.id'] = "ruby_kafka_consumer_#{Time.now.to_i}"
    opts.each {|k,v| @config[k] = v}
    set_deserializers(key_deserializer, value_deserializer)

    @consumer = Java::KafkaConsumer::Consumer.createJavaConsumerConnector(Java::KafkaConsumer::ConsumerConfig.new @config)
    filter = Java::KafkaConsumer::Whitelist.new topic
    stream = @consumer.create_message_streams_by_filter(filter, 1, @key_deserializer, @value_deserializer).get 0
    iterator = stream.iterator
    @thread = get_reader_thread(iterator)
  end

  def get_message(timeout=60)
    start = Time.now
    message = nil
    while message.nil? && Time.now < (start + timeout)
      begin
        message = @queue.pop(true)
      rescue ThreadError #standard queue exception when not blocking and empty
        sleep 0.5
      end
    end
    return message
  end

  def clear_all_messages
    @queue.clear
  end

  def get_reader_thread(iterator)
    Thread.new do
      while iterator.has_next?
        begin
          entry = iterator.next
          @queue << {value: entry.message, key: entry.key, offset: entry.offset, partition: entry.partition}
        rescue Java::OrgApacheKafkaCommonErrors::SerializationException => e
          puts "Error reading message: #{e.message} => #{e.cause}"
        rescue StandardError => e
          puts "Error reading message: #{e.message}"
        end
      end
    end
  end
  private :get_reader_thread

  def set_deserializers(key_deserializer, value_deserializer)
    if key_deserializer.to_s == "avro" || value_deserializer.to_s == "avro"
      raise ArgumentError, "'schema.registry.url' option required with avro serializer" unless @config['schema.registry.url']
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
      @key_deserializer = Java::KafkaSerializer::DefaultDecoder.new nil
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
      @value_deserializer = Java::KafkaSerializer::DefaultDecoder.new nil
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
