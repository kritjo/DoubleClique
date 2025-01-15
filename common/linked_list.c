#include "linked_list.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

void insert_node(node_t **head, char *key, void *data) {
    node_t *node = malloc(sizeof(node_t));
    if (node == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    node->key = malloc(strlen(key) + 1);
    node->data = data;
    strcpy(node->key, key);

    if (*head == NULL) {
        *head = node;
        (*head)->next = NULL;
        (*head)->prev = NULL;
        return;
    }

    (*head)->prev = node;
    node->next = (*head);
    node->prev = NULL;
    (*head) = node;
}

node_t *find_node(node_t **head, char *key) {
    node_t *node = (*head);
    while (node != NULL) {
        if (strcmp(node->key, key) == 0) break;
        node = node->next;
    }
    return node;
}

void delete_node_by_key(node_t **head, char *key, void (*free_func)(node_t *free_node)) {
    node_t *node = (*head);
    while (node != NULL) {
        if (strcmp(node->key, key) == 0) break;
        node = node->next;
    }
    if (node == NULL) return;
    delete_node(head, node, free_func);
}

void delete_node(node_t **head, node_t *node, void (*free_func)(node_t *free_node)) {
    if (node->prev == 0) {
        (*head) = node->next;
        if ((*head) != NULL) (*head)->prev = 0;
        if (free_func != NULL) free_func(node);
        free(node->key);
        free(node);
    } else if (node->next == 0) {
        node->prev->next = 0;
        if (free_func != NULL) free_func(node);
        free(node->key);
        free(node);
    } else {
        node->prev->next = node->next;
        node->next->prev = node->prev;
        if (free_func != NULL) free_func(node);
        free(node->key);
        free(node);
    }
}

void delete_all_nodes(node_t **head, void (*free_func)(node_t *free_node)) {
    node_t *node = (*head);
    while (node != NULL) {
        node_t *tmp = node;
        node = node->next;
        if (free_func != NULL) free_func(tmp);
        free(tmp->key);
        free(tmp);
    }
}

void free_generic(node_t *node) { free(node->data); }
