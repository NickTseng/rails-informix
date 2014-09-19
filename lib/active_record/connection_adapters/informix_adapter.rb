# Copyright (c) 2006-2010, Gerardo Santana Gomez Garrido <gerardo.santana@gmail.com>
# Rails 4 additions by Jihwan Song (jihwans@github)
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. The name of the author may not be used to endorse or promote products
#    derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/statement_pool'
require 'arel/visitors/informix'
require 'arel/visitors/bind_visitor'

gem 'ruby-informix', '~> 0.8'
require 'informix'

module Informix

    #
    # <tt>Informix::Result</tt> is a class needed to make sense out of the
    # <tt>Informix#cursor()</tt> return value and adjust it in a way
    # that is palatable to <tt>ActiveRecord::Result</tt> objects, namely
    # to carry the +fields+ and the +to_a+ methods properly
    #
    class Result

      include Enumerable

      attr_reader :cursor_result

      #
      # +cursor_result+ must be in the form of an array of hashes
      # in which each hash represents a row returned by the database.
      # The hash has the fields names as keys and the vaules for that row as
      # values.
      #
      # Trivial Example:
      #
      # <tt>
      #    cursor_results = [{ "COUNT(*) => 33" }]
      #    ir = Informix::Result.new(cursor_results)
      #    ir.fields => ["COUNT(*)"]
      #    ir.values => [33]
      # </tt>
      #
      def initialize(cr)
        @cursor_result = cr
      end

      def fields
        res = []
        res = self.cursor_result.first.keys unless self.cursor_result.empty?
      end

      def to_a
        self.cursor_result.map { |row| row.values }
      end

    end # class Result
end # module Informix

module ActiveRecord
  module ConnectionHandling # :nodoc:
    def informix_connection(config) #:nodoc:
      ENV['DBDATE'] = 'Y4MD-' 
      ConnectionAdapters::InformixAdapter.new(nil, logger, nil, config)
    end
  end #module ConnectionHandling

  # This thing looks like a blob related headache...
  # It does not look right to be here... do something from child class, only when necessary...
  # require 'stringio'
  # class Base
  #   after_save :write_lobs
  #   def write_lobs
  #     return unless connection.is_a?(Informix::Database)
  #     self.class.columns.each do |c|
  #       value = self[c.name]
  #       next if (![:text, :binary].include? c.type) || value.nil? || value == ''
  #       connection.raw_connection.execute(<<-end_sql, StringIO.new(value))
  #           UPDATE #{self.class.table_name} SET #{c.name} = ?
  #           WHERE #{self.class.primary_key} = #{quote_value(id)}
  #       end_sql
  #     end
  #   end
  # end

  module ConnectionAdapters
    class InformixColumn < Column
      def initialize(column)
        sql_type = make_type(column[:stype], column[:length],
                             column[:precision], column[:scale])
        super(column[:name], column[:default], sql_type, column[:nullable])
      end
      private
        IFX_TYPES_SUBSET = %w(CHAR CHARACTER CHARACTER\ VARYING DECIMAL FLOAT
                              LIST LVARCHAR MONEY MULTISET NCHAR NUMERIC
                              NVARCHAR SERIAL SERIAL8 VARCHAR).freeze

        def make_type(type, limit, prec, scale)
          type.sub!(/money/i, 'DECIMAL')
          if IFX_TYPES_SUBSET.include? type.upcase
            if prec == 0
              "#{type}(#{limit})" 
            else
              "#{type}(#{prec},#{scale})"
            end
          elsif type =~ /datetime/i
            type = "time" if prec == 6
            type
          elsif type =~ /byte/i
            "binary"
          else
            type
          end
        end

        def simplified_type(sql_type)
          if sql_type =~ /serial/i
            :primary_key
          else
            super
          end
        end
    end

    # This adapter requires Ruby/Informix
    # http://ruby-informix.rubyforge.org
    #
    # Options:
    #
    # * <tt>:database</tt>  -- Defaults to nothing.
    # * <tt>:host</tt>      -- (Optional) host name
    # * <tt>:username</tt>  -- Defaults to nothing.
    # * <tt>:password</tt>  -- Defaults to nothing.
    # * <tt>:nolog</tt> - no transaction log option - no not use "begin work"
    # * <tt>:nomig</tt> - no migration option

    class InformixAdapter < AbstractAdapter
      ADAPTER_NAME = 'Informix'
      NATIVE_DATABASE_TYPES = {
        :primary_key => "serial primary key",
        :string      => { :name => "varchar", :limit => 255  },
        :text        => { :name => "text" },
        :integer     => { :name => "integer" },
        :float       => { :name => "float" },
        :decimal     => { :name => "decimal" },
        :datetime    => { :name => "datetime year to second" },
        :timestamp   => { :name => "datetime year to second" },
        :time        => { :name => "datetime hour to second" },
        :date        => { :name => "date" },
        :binary      => { :name => "byte"},
        :boolean     => { :name => "boolean"}
      }
      QUOTED_TRUE, QUOTED_FALSE = %q{'t'}, %q{'f'}

      attr_reader :last_return_value

      class BindSubstitution < Arel::Visitors::Informix # :nodoc:
        include Arel::Visitors::BindVisitor
      end

      class StatementPool < ConnectionAdapters::StatementPool
        def initialize(connection, max = 1000)
          super
          @cache = Hash.new { |h,pid| h[pid] = {} }
        end

        def each(&block); cache.each(&block); end
        def key?(key);    cache.key?(key); end
        def [](key);      cache[key]; end
        def length;       cache.length; end
        def delete(key);  cache.delete(key); end

        def []=(sql, key)
          while @max <= cache.size
            cache.shift.last[:stmt].close
          end
          cache[sql] = key
        end

        def clear
          cache.values.each do |hash|
            hash[:stmt].close
          end
          cache.clear
        end

        private
        def cache
          @cache[Process.pid]
        end
      end

      # either config, logger -- or -- 
      def initialize(db, logger, connection_parameters = nil, config = nil)
        super(db, logger)
        config.symbolize_keys!
        config.delete_if { |_, v| v.nil? }

        database    = config[:database].to_s
        database    << "@#{config[:host]}" if config.has_key?(:host)
        username    = config[:username]
        password    = config[:password]
        #
        # We basically avoid +prepared_statements+ because we can't find
        # documentation on what they are (we *suppose* something, but that's
        # simply not enough down here) and how to implement them in
        # +Informix+. However, they can be set to true in the configuration,
        # so you may want to test them by setting:
        #
        #     prepared_statements:  true
        #
        # in the database configuration
        #
        if config.fetch(:prepared_statements) { false }
          @visitor = Arel::Visitors::Informix.new self
        else
          @visitor = BindSubstitution.new self
        end

        c = Informix.connect(
          config[:database],
          config[:username],
          config[:password]
        )
        @ifx_version = c.version.major.to_i

        @statements = StatementPool.new(
          c, self.class.type_cast_config_to_integer(
              config.fetch(:statement_limit) { 1000 }
            )
          )
        # @connection_parameters, @config = connection_parameters, config
        @config = config
        @connection = c
      end

      def native_database_types
        NATIVE_DATABASE_TYPES
      end

      def adapter_name
        ADAPTER_NAME
      end

      def supports_migrations? #:nodoc:
        !@config[:nomig]
      end

      # DATABASE STATEMENTS =====================================

      # Executes the SQL statement in the context of this connection.
      # -- expects no return value
      def execute(sql, name = nil)
        log(sql, name) { @last_return_value = @connection.immediate(sql) }
      end

      # Executes SQL, expects returns
      def exec_query(sql, name = nil, binds = [])
        result = nil
        get_cursor(sql, name, binds) do
          |cursor|
          result = Informix::Result.new(cursor.open.fetch_hash_all)
        end
        ActiveRecord::Result.new(result.fields, result.to_a) if result
      end

      def get_cursor(sql, name = nil, binds = [])
        log(sql, name, binds) do
          cursor = @connection.cursor(sql)
          yield(cursor) if block_given?
          cursor.free
        end
      end

      alias :select :exec_query

      def insert_sql(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil)
        execute(sql, name)
        r = exec_query("select first 1 dbinfo('sqlca.sqlerrd1') from systables")
        r.rows[0][0] unless r.nil?
      end

      def exec_insert(sql, name, binds, pk = nil, sequence_name = nil)
        #exec_query(sql, name, binds)
        insert_sql sql, name, pk, nil, sequence_name
      end

      def prepare(sql, name = nil, binds = [])
        log(sql, name, binds) { @connection.prepare(sql) }
      end

      def last_inserted_id(result)
        nil
      end

      def begin_db_transaction
        execute("begin work") unless @config[:nolog]
      end

      def commit_db_transaction
        @connection.commit unless @config[:nolog]
      end

      def rollback_db_transaction
        @connection.rollback unless @config[:nolog]
      end
      
      def primary_key(table_name) #:nodoc:
        res = nil
        @connection.cursor(<<-end_sql) do |cur|
            SELECT FIRST 1 ct.constrname FROM sysconstraints ct, systables st WHERE st.tabid = ct.tabid AND ct.constrtype = 'P' AND st.tabname = '#{table_name}'
          end_sql
          rows = cur.open.fetch
          res = rows.first if rows
        end
        res
      end

      # QUOTING ===========================================
      def quote_string(string)
        string.gsub(/\'/, "''")
      end

      def quote(value, column = nil)
        if column && [:binary, :text].include?(column.type)
          return "NULL"
        end
        if column && column.type == :date
          return "'#{value.mon}/#{value.day}/#{value.year}'"
        end
        super
      end

      # super
      #def quoted_true
      #  QUOTED_TRUE
      #end

      def unquoted_true
        1
      end

      # super
      #def quoted_false
      #  QUOTED_FALSE
      #end

      def unquoted_false
        0
      end

      # SCHEMA STATEMENTS =====================================
      def tables(name = nil)
        @connection.cursor(<<-end_sql) do |cur|
            SELECT tabname FROM systables WHERE tabid > 99 AND tabtype != 'Q'
          end_sql
          cur.open.fetch_all.flatten
        end
      end

      def columns(table_name, name = nil)
        @connection.columns(table_name).map {|col| InformixColumn.new(col) }
      end

      # MIGRATION =========================================
      #
      # These calls cannot be done from an open connection, so they
      # won't work in any case.
      #
      # def recreate_database(name)
      #   drop_database(name)
      #   create_database(name)
      # end

      # def drop_database(name)
      #   execute("drop database #{name}")
      # end

      # def create_database(name)
      #   execute("create database #{name}")
      # end

      def create_table(name, options = {})
        super(name, options)
      end

      def drop_table(name, options = {})
        super(name, options)
      end

      # XXX
      def indexes(table_name, name = nil)
        indexes = []
      end
            
      def rename_column(table, column, new_column_name)
        execute("RENAME COLUMN #{table}.#{column} TO #{new_column_name}")
      end
      
      def change_column(table_name, column_name, type, options = {}) #:nodoc:
        sql = "ALTER TABLE #{table_name} MODIFY #{column_name} #{type_to_sql(type, options[:limit])}"
        add_column_options!(sql, options)
        execute(sql)
      end

      def remove_index(table_name, options = {})
        execute("DROP INDEX #{index_name(table_name, options)}")
      end

      def delete(dstmt, name = nil, binds = [])
        execute(to_sql(dstmt, binds), name, binds)
      end

    end #class InformixAdapter < AbstractAdapter
  end #module ConnectionAdapters
end #module ActiveRecord
