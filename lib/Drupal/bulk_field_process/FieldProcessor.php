<?php

namespace Drupal\bulk_field_process;

/**
 * FieldProcessor class to perform operations on Drupal/eck fields.
 */
class FieldProcessor {
  /**
   * Output debug messages.
   *
   * @var bool
   */
  public static $verbose = FALSE;

  /**
   * Just a wrapper to print messages in a context-aware way.
   *
   * @param string $message
   *   The message to print.
   * @param string $type
   *   The message type to display with the message.
   */
  public static function printMessage($message, $type = 'info') {
    // Is it CLI/Drush?
    $timestamp = date('Y-m-d H:i:s');
    $message = "[$timestamp][$type] $message";
    if (drupal_is_cli()) {
      if (function_exists('drush_main')) {
        drush_print($message);
      }
      // Cli without drush.
      else {
        echo $message . PHP_EOL;
      }
    }
    // Web context.
    else {
      drupal_set_message($message);
    }
  }

  /**
   * Return fields that match the given field types.
   *
   * @param array $types
   *   The type(s) of fields to find.
   *
   * @return array
   *   Array with key => value being field name => field info.
   */
  public static function getFieldsOfType(array $types) {
    $matched_fields = [];

    $fields_info = field_info_fields();
    foreach ($fields_info as $field_name => $field_info) {
      if (empty($types) || in_array($field_info['type'], $types)) {
        $matched_fields[$field_name] = $field_info;
      }
    }

    return $matched_fields;
  }

  /**
   * Find tables associated with a field type.
   *
   * @param array $types
   *   Field types, e.g. text, entityreference.
   * @param bool $revision_tables
   *   Include the revision tables.
   *
   * @return array
   *   Array of tables associated with the field types.
   */
  public static function getFieldTables(array $types = [], $revision_tables = TRUE) {
    $tables = [];
    $table_types = $revision_tables ? [FIELD_LOAD_CURRENT, FIELD_LOAD_REVISION] : [FIELD_LOAD_CURRENT];
    $fields_info = self::getFieldsOfType($types);
    foreach ($fields_info as $field_info) {
      foreach ($table_types as $table_type) {
        foreach ($field_info['storage']['details']['sql'][$table_type] as $table_name => $columns) {
          $tables[$table_name] = array_intersect_key($columns, array_flip([
            'value',
            'summary',
            'target_id',
          ]));
        }
      }
    }

    return $tables;
  }

  /**
   * The core of this class. Run a callback on all matching fields.
   *
   * For field level processing ($process_fields = TRUE) the signature should
   * match function(string $text, object $row). For row level processing, the
   * signature should be function(string $text, object $row, string $table).
   *
   * @param callable $callback
   *   The callback function to run.
   * @param array $types
   *   The field types to process on.
   * @param array $search_pattern
   *   Patterns to match field content against. e.g. ['href=', '[uniquetoken]'].
   * @param bool $revision_tables
   *   Include revision tables.
   * @param bool $process_fields
   *   Run callback on field data, if FALSE, run only against the row.
   * @param bool $exact_match
   *   Use LIKE '%pattern%' if TRUE, otherwise ='pattern'.
   */
  public static function process(callable $callback, array $types = [], array $search_pattern = [], $revision_tables = TRUE, $process_fields = TRUE, $exact_match = FALSE) {
    if (empty($types)) {
      $types = [
        'text',
        'text_long',
        'text_with_summary',
      ];
    }
    $tables = self::getFieldTables($types, $revision_tables);

    foreach ($tables as $table_name => $columns) {
      if (self::$verbose) {
        self::printMessage("Processing table: $table_name");
      }
      self::processTable($table_name, $columns, $search_pattern, $callback, $process_fields, $exact_match);
    }
  }

  /**
   * Find entities with matching content in fields.
   *
   * @param array $types
   *   Array of type strings.
   * @param array $search_pattern
   *   Array of search pattern strings.
   * @param bool $include_field_data
   *   If true, include the field contents, otherwise return only id/info.
   * @param bool $revisions
   *   Include revisions if TRUE, otherwise just the main/published entity.
   * @param bool $exact_match
   *   Search for the field value equal to the patterns, otherwise LIKE %pat%.
   *
   * @return array
   *   Array of nodes and revisions, with optional field data.
   */
  public static function find(array $types = [], array $search_pattern = [], $include_field_data = FALSE, $revisions = TRUE, $exact_match = FALSE) {
    $results = [];

    self::process(function ($data, $row, $table) use (&$results, $include_field_data) {
      if (!isset($results[$row->entity_id])) {
        $results[$row->entity_id] = [];
      }

      if (!isset($results[$row->entity_id][$row->revision_id])) {
        $results[$row->entity_id][$row->revision_id] = [];
      }

      if ($include_field_data) {
        $results[$row->entity_id][$row->revision_id][$table] = $row;
      }
      else {
        $results[$row->entity_id][$row->revision_id] = [
          'entity_type' => $row->entity_type,
          'bundle' => $row->bundle,
          'entity_id' => $row->entity_id,
          'revision_id' => $row->revision_id,
        ];
      }
    }, $types, $search_pattern, $revisions, FALSE, $exact_match);

    return $results;
  }

  /**
   * Run callback on the relevant fields in a row.
   *
   * @param callable $callback
   *   Callback function.
   * @param object $row
   *   The row object.
   * @param array $field_names
   *   Field names to run callback function on.
   *
   * @return bool
   *   TRUE if data was replaced.
   */
  protected static function processFields(callable $callback, $row, $field_names) {
    $replaced = FALSE;
    foreach ($field_names as $field_name) {
      $replacement = call_user_func($callback, $row->{$field_name}, $row);
      if ($replacement != $row->{$field_name}) {
        $row->{$field_name} = $replacement;
        $replaced = TRUE;
      }
    }

    return $replaced;
  }

  /**
   * Find fields in specified table with data that matches the given patterns.
   *
   * @param string $table
   *   The table to process.
   * @param array $columns
   *   Columns that contain data to search.
   * @param array $patterns
   *   Patterns to search.
   * @param callable $callback
   *   Callback function to run against.
   * @param bool $process_fields
   *   Process on field level if TRUE, row level if FALSE.
   * @param bool $exact_match
   *   Match with LIKE if FALSE otherwise =.
   */
  protected static function processTable($table, $columns, array $patterns, $callback, $process_fields = TRUE, $exact_match = FALSE) {
    $query = db_select($table, $table)
      ->fields($table, [
        'entity_id',
        'revision_id',
        'language',
        'deleted',
        'delta',
        'entity_type',
        'bundle',
      ])
      ->fields($table, $columns);

    if (!empty($patterns)) {
      $condition = db_or();
      foreach ($columns as $column) {
        foreach ($patterns as $pattern) {
          if (!$exact_match) {
            $condition->condition($column, '%' . db_like($pattern) . '%', 'LIKE');
          }
          else {
            $condition->condition($column, $pattern, '=');
          }
        }
      }
      $query->condition($condition);
    }

    $result = $query->execute();
    foreach ($result as $row) {
      if (!$process_fields) {
        // Just send the row to the callback function. Data/Text is NULL
        // because we're not processing on a field level.
        $callback(NULL, $row, $table);
        continue;
      }
      $result = self::processFields($callback, $row, $columns);
      if ($result) {
        $update_query = db_update($table)
          ->condition('entity_id', $row->entity_id)
          ->condition('revision_id', $row->revision_id);

        $updated_values = [];
        foreach ($columns as $column) {
          $updated_values[$column] = $row->{$column};
        }
        $update_query->fields($updated_values);
        $update_query->execute();
        if (self::$verbose) {
          self::printMessage("Updated values for entity id: {$row->entity_id} revision id: {$row->revision_id}. Table: $table");
        }
      }
      elseif (self::$verbose) {
        self::printMessage("No update: for entity id: {$row->entity_id} revision id: {$row->revision_id}. Table: $table");
      }
    }
  }

}
