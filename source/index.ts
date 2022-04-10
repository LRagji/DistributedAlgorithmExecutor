export class DAE {
    constructor(private redis: IRedisClient) {
    }

    public addStep(step: IStep, async = false, consumerNames: string[] = [], maximumTime: number = -1): Promise<boolean> {
        throw new Error("W.I.P");
    }

    public fetchNextStep(algorithmName: string, consumerName: string): Promise<IFetchedStep | null> {
        throw new Error("W.I.P");
    }

    public markAlgorithmCompleted(algorithmName: string): Promise<boolean> {
        throw new Error("W.I.P");
    }

    public algorithmStatus(algorithmName: string): Promise<any> {
        throw new Error("W.I.P");
    }

    /**
     * This method helps to populate variables that can be used within an algorithm steps and are common across.
     * It can throw when parameter "algorithmName" does not exits or if algorithm is marked as completed using {@link markAlgorithmCompleted | markAlgorithmCompleted method}
     */
    public populateAlgorithmVariables(algorithmName: string, variables: Map<string, string>): Promise<boolean> {
        throw new Error("W.I.P");
    }
}

export interface IStepArgument {
    type: "Literal" | "Reference" | "Resolved"
    value: string
}

export interface IFetchedStep extends IStep {
    return: (value: string) => Promise<boolean>
}

export interface IStep {
    opCode: string
    algorithmName: string
    stepName: string
    args: Array<IStepArgument>
}

export interface IRedisClient {
    acquire(token?: string): Promise<void>
    release(token?: string): Promise<void>
    shutdown(): Promise<void>
    run(commandArgs: string[]): Promise<any>
    pipeline(commands: string[][]): Promise<any>;
    script(filename: string, keys: string[], args: string[]): Promise<any>
}

interface IRedisStep extends IStep {
    consumers: Array<string>
    async: number
    maximumTime: number
}