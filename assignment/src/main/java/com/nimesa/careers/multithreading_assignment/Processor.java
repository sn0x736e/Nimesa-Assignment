package com.nimesa.careers.multithreading_assignment;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class Processor {

    private final Queue<TaskRequest> queue = new LinkedBlockingQueue<>();


    Processor(TaskRequest taskRequest) {
        queue.offer(taskRequest);
    }

    Processor(List<TaskRequest> taskRequest) {
        for (TaskRequest request : taskRequest) {
            queue.offer(request);

        }
    }

    public List<TaskResponse> execute() throws InterruptedException {
        List<TaskResponse> taskResponses = new ArrayList<>();
        /* All Task in outer List will be executed in parallel in respect to each other and All Task within Inner List will be executed sequentially in respect to each other in inner list according to priority */
        List<List<TaskRequest>> groupedTasks = new ArrayList<>();
        groupTaskForParallelExecution(groupedTasks);
        executeTaskInParallel(groupedTasks, taskResponses);
        return taskResponses;
    }

    private void groupTaskForParallelExecution(List<List<TaskRequest>> groupedTasks) {
        /* First group by User, then group by Task Type, then sort by Priority */
        queue.stream().collect(Collectors.groupingBy(TaskRequest::getSubmittedBy))
                        .values().forEach(sameUserTaskRequests -> {
                            sameUserTaskRequests.stream().collect(Collectors.groupingBy(TaskRequest::getType))
                                    .values().forEach(sameUserTaskTypeRequests -> {
                                        sameUserTaskTypeRequests.sort(Comparator.comparing(TaskRequest::getPriority));
                                        if(!sameUserTaskTypeRequests.isEmpty()){
                                            groupedTasks.add(sameUserTaskTypeRequests);
                                        }
                                    });
                });
    }

    private static void executeTaskInParallel(List<List<TaskRequest>> groupedTasks, List<TaskResponse> taskResponses) {
        /* Using Parallel Stream to run each Inner TaskRequest List in Parallel */
        groupedTasks.parallelStream().forEach(taskRequestList -> {
            taskRequestList.forEach(taskRequest -> {
                Task task = new Task(taskRequest);
                try {
                    System.out.println("Starting Task "+taskRequest.getId());
                    TaskResponse response = task.run();
                    System.out.println("Completed Task "+response.getId() +" With Status "+response.getStatus());
                    taskResponses.add(response);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        });
    }

}
