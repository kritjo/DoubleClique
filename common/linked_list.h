#ifndef DOUBLECLIQUE_LINKED_LIST_H
#define DOUBLECLIQUE_LINKED_LIST_H

#define FREE_DATA free_generic

typedef struct node {
    char *key;
    void *data;
    struct node *next;
    struct node *prev;
} node_t;

void delete_node_by_key(node_t **head, char *key, void (*free_func)(node_t *free_node));

node_t *find_node(node_t **head, char *key);

void insert_node(node_t **head, char *key, void *data);

void delete_all_nodes(node_t **head, void (*free_func)(node_t *free_node));

void delete_node(node_t **head, node_t *node, void (*free_func)(node_t *free_node));

void free_generic(node_t *node);

#endif //DOUBLECLIQUE_LINKED_LIST_H
